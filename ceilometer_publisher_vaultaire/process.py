import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc

from marquise import Marquise

import ceilometer_publisher_vaultaire.siphash

KEYS_TO_DELETE = [
    "timestamp",
    "volume",
    "created_at",
    "updated_at",
    "id",
    "size",
]

RAW_PAYLOAD_IP_ALLOC = 1

def process_sample(sample):
    processed = []
    consolidated_sample = None
    try:
        if "event_type" in sample["resource_metadata"]:
            consolidated_sample = process_consolidated_event(sample)
        else:
            consolidated_sample = process_consolidated_pollster(sample)
    except Exception:
        # log here
        pass
    if consolidated_sample is not None:
        processed.append(consolidated_sample)
    processed.append(process_raw(sample))
    return processed

def remove_extraneous(sourcedict):
    for k in KEYS_TO_DELETE:
        sourcedict.pop(k, None)

def process_raw(sample):
    """
    Process a sample into a raw (not consolidated) datapoint. Makes
    destructive updates to sample.
    """
    event_type = sample["resource_metadata"].get("event_type", None)

    flavor_type = get_flavor_type(sample)

    address = get_address(sample, sample["name"], consolidated=False)

    # Sanitize timestamp (will parse timestamp to nanoseconds since epoch)
    timestamp = sanitize_timestamp(sample["timestamp"])

    # Our payload is the volume (later parsed to
    # "counter_volume" in ceilometer)
    payload = sanitize(sample["volume"])

    # Rebuild the sample as a source dict
    sourcedict = dict(sample)

    # Vaultaire cares about the datatype of the payload
    if type(payload) == float:
        sourcedict["_float"] = 1

    # Cast unit as a special metadata type
    sourcedict["_unit"] = sanitize(sourcedict.pop("unit", None))

    # If it's a cumulative value, we need to tell vaultaire
    if sourcedict["type"] == "cumulative":
        sourcedict["_counter"] = 1

    # Cast Identifier sections with unique names, in case of
    # metadata overlap

    # XXX: why do we call these counter_* even when they're
    # not counters?
    sourcedict["counter_name"] = sanitize(sourcedict.pop("name"))
    sourcedict["counter_type"] = sanitize(sourcedict.pop("type"))

    remove_extraneous(sourcedict)

    # Remove the original resource_metadata and substitute
    # our own flattened version
    sourcedict.update(flatten(sourcedict.pop("resource_metadata")))
    sourcedict = flatten(sourcedict)
    for k, v in sourcedict.items():
        sourcedict[k] = sanitize(str(v))
    return (address, sourcedict, timestamp, payload)

def is_event_sample(sample):
    return "event_type" in sample["resource_metadata"]

def get_base_sourcedict(payload, sample, name, consolidated=False):
    sourcedict = {}
    sourcedict["_event"]        = 1 if is_event_sample(sample) else 0
    sourcedict["_consolidated"] = 1 if consolidated else 0
    sourcedict["project_id"]    = sample["project_id"]
    sourcedict["resource_id"]   = sample["resource_id"]
    sourcedict["counter_name"]  = name
    sourcedict["counter_unit"]  = sample["unit"]
    sourcedict["counter_type"]  = sample["type"]
    sourcedict["display_name"]  = sample["resource_metadata"].get("display_name", "")

    if sample["type"] == "cumulative":
        sourcedict["_counter"] = 1

    if type(payload) == float:
        sourcedict["_float"] = 1

    for k, v in sourcedict.items():
        sourcedict[k] = sanitize(str(v))

    return sourcedict

def get_address(sample, name, consolidated=False):
    """
    get_address returns a unique Vaultaire address calculated from the
    supplied id_elements, which should be a list of strings considered part
    of the primary key for the relevant metric.
    """
    id_elements = get_id_elements(sample, name, consolidated)
    id_elements = [ str(x) for x in id_elements if x is not None ]
    identifier = "".join(id_elements)
    return Marquise.hash_identifier(identifier)

def process_consolidated_pollster(sample):
    # Pull out and clean fields which are always present
    name         = sample["name"]
    project_id   = sample["project_id"]
    resource_id  = sample["resource_id"]
    metadata     = sample["resource_metadata"]
    ## Cast unit as a special metadata type
    counter_unit = sample["unit"]
    counter_type = sample["type"]
    ## Sanitize timestamp (will parse timestamp to nanoseconds since epoch)
    timestamp    = sanitize_timestamp(sample["timestamp"])

    # We use a siphash of the instance_type for instance pollsters
    if name.startswith("instance"):
        payload = hash_flavor_id(metadata["instance_type"])
    elif name.startswith("volume.size"):
        payload = volume_to_raw_payload(sanitize(sample["volume"]))
    elif name.startswith("ip.floating"):
        payload = RAW_PAYLOAD_IP_ALLOC
    else:
        # We don't need to write out a consolidated version of anything
        # else.
        return None

    sourcedict = get_base_sourcedict(payload, sample, name, consolidated=True)

    # Common elements to all messages are r_id + p_id + counter_(name,
    # type, unit)
    address = get_address(sample, name, consolidated=True)

    # Filter out Nones and stringify everything so we don't get
    # TypeErrors on concatenation
    return (address, sourcedict, timestamp, payload)

def get_id_elements(sample, name, consolidated):
    """
    get_id_elements returns a list of components uniquely identifying a
    metric.
    """
    id_elements = [
        sample["project_id"],
        sample["resource_id"],
        sample["unit"],
        sample["type"],
        name,
    ]
    if consolidated:
        id_elements.append("_consolidated")
    # If this is an event sample, the event type is always part of the
    # identifier (as is the fact that it is an event).
    if is_event_sample(sample):
        id_elements.append("_event")
        id_elements.append(sample["resource_metadata"]["event_type"])
    return id_elements

def process_consolidated_event(sample):
    # Pull out and clean fields which are always present
    name         = sample["name"]
    project_id   = sample["project_id"]
    resource_id  = sample["resource_id"]
    metadata     = sample["resource_metadata"]
    ## Cast unit as a special metadata type
    counter_unit = sample["unit"]
    counter_type = sample["type"]
    ## Sanitize timestamp (will parse timestamp to nanoseconds since epoch)
    timestamp    = sanitize_timestamp(sample["timestamp"])

    # We use the instance_type_id for instance events
    if name.startswith("instance"):
        payload = get_consolidated_payload(metadata["event_type"], metadata.get("message",""), metadata["instance_type_id"])
    elif name.startswith("volume.size"):
        payload = get_consolidated_payload(metadata["event_type"], metadata.get("status",""), volume_to_raw_payload(sample["volume"]))
    elif name.startswith("ip.floating"):
        payload = get_consolidated_payload(metadata["event_type"], "", RAW_PAYLOAD_IP_ALLOC)
    else:
        payload = sanitize(sample["volume"])

    sourcedict = get_base_sourcedict(payload, sample, name, consolidated=True)

    address = get_address(sample, name, consolidated=True)

    return (address, sourcedict, timestamp, payload)

def sanitize(v):
    """Sanitize a value into something that Marquise can use. This weeds
    out None keys/values, and ensures that timestamps are consistently
    formatted.
    """
    if v is None:
        return ""
    if type(v) is bool:
        if v:
            return 1
        else:
            return 0
    if type(v) is unicode:
        v = str(v)
    if type(v) is str: # XXX: will be incorrect/ambiguous in Python 3.
        v = v.replace(":","-")
        v = v.replace(",","-")
    return v

def flatten(n, prefix=""):
    """Take a (potentially) nested dictionary and flatten it into a single
    level. Also remove any keys/values that Marquise/Vaultaire can't handle.
    """
    flattened_dict = {}
    for k,v in n.items():
        k = sanitize(k)

        # Vaultaire doesn't care about generated URLs for Ceilometer
        # API references.
        if str(k) == "links":
            continue
        if str(k).endswith('_url'):
            continue

        # Vaultaire doesn't want values if they have no contents.
        if v is None:
            continue

        # If key has a parent, concatenate it into the new keyname.
        if prefix != "":
            k = "{}-{}".format(prefix,k)

        # This was previously a check for __iter__, but strings have those now,
        # so let's just check for dict-ness instead. No good on lists anyway.
        if type(v) is not dict:
            v = sanitize(v)
            flattened_dict[k] = v
        else:
            flattened_dict.update(flatten(v, k))
    return flattened_dict

def sanitize_timestamp(v):
    """Convert a timestamp value into a standard form. Does no exception
    handling, as we really want to know if this fails."""
    if type(v) is unicode:
        v = str(v)
    # Try and take a value and use dateutil to parse it.  If there's no TZ
    # spec in the string, assume it's UTC because that's what Ceilometer
    # uses.
    # Eg. 2014-08-10T12:14:13Z      # timezone-aware
    # Eg. 2014-08-10 12:14:13       # timezone-naive
    # Eg. 2014-08-10 12:14:13+1000  # timezone-aware
    NANOSECONDS_PER_SECOND = 10**9
    if type(v) is datetime.datetime:
        timestamp = v
    else: # We have a string.
        timestamp = parse(v)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=tzutc())
    # If we get here, we've successfully grabbed a datetime.
    # FIXME: use strftime
    epoch = datetime.datetime(1970, 1, 1, tzinfo=tzutc())
    time_since_epoch = (timestamp - epoch).total_seconds() # total_seconds() is in Py2.7 and later.
    return int(time_since_epoch * NANOSECONDS_PER_SECOND)

def hash_flavor_id(flavor_id):
    """
    hash_flavor_id will return the SipHash-2-4 of a string with a null
    key.

    We do this to instance types before we store them because a)
    instance types are strings and b) we can't assume they won't change
    (ceilometer doesn't give us easy access to the guaranteed-integral
    instance_type_id).
    """
    return ceilometer_publisher_vaultaire.siphash.SipHash24("\0"*16, flavor_id).hash()

def get_flavor_type(sample):
    flavor_type = None
    if "flavor" in sample:
        flavor_type = sample["flavor"].get("name", None)
    elif "instance_type" in sample:
        flavor_type = sample["instance_type"]
    return flavor_type


def get_consolidated_payload(event_type, message, rawPayload):
    """
    eventResolution is passed in message, if none is given assumed to be Success
    eventResolution takes up the LSByte
    eventVerb is the action of the event, e.g. create, delete, shutdown, etc.
    eventVerb takes up the 2nd LSByte
    eventEndpoint defines whether the data represents an event start, end or an
    instance event.
    eventVerb takes up the 3rd LSByte
    rawPayload takes up the 4 MSBytes (32 bits)
    rawPayload is specific on counter
    Start events are even, end are odd (LSB)
    Other 7 bits for create, update, end, and any future additions
    """

    # If no event endpoint is included, assume instantaneous event (e.g. image deletion)
    eventEndpoint = 0

    if message == "Success":
        eventResolution = 0
    # Empty message implies success
    elif message == "":
        eventResolution = 0
    elif message == "Failure":
        eventResolution = 1
    elif message == "error":
        eventResolution = 2

    rest,maybeVerb = event_type.rsplit('.', 1)
    if maybeVerb in ("start", "end"):
        eventEndpoint = {"start":1, "end":2}.get(maybeVerb)
        _,verb = rest.rsplit('.', 1)
    else:
        verb = maybeVerb

    eventVerb = {"create":1, "update":2, "delete":3, "shutdown":4, "exists":5}.get(verb)

    if eventResolution is None:
        raise Exception("Unsupported message given to get_consolidated_payload")
    if eventVerb is None:
        raise Exception("Unsupported event class given to get_consolidated_payload")
    if eventEndpoint is None:
        raise Exception("Unsupported event endpoint given to get_consolidated_payload")

    return eventResolution + (eventVerb << 8) + (eventEndpoint << 16) + (rawPayload << 32)

def volume_to_raw_payload(volume):
    return volume
