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
    "resource_metadata",
    "flavor",
]

RAW_PAYLOAD_IP_ALLOC = 1

def process_sample(sample):
    processed = []
    consolidated_sample = None
    name = sample["name"]
    # We want to do some more processing to these event types...
    if "event_type" in sample["resource_metadata"]:
        if name.startswith("instance"):
            processed.append(consolidate_instance_event(sample))
        elif name.startswith("volume.size"):
            processed.append(consolidate_volume_event(sample))
        elif name.startswith("ip.floating"):
            processed.append(consolidate_ip_event(sample))
    # ...and this pollster type.
    elif name.startswith("instance"):
        processed.append(consolidate_instance_flavor(sample))
        processed.append(consolidate_instance_ram(sample))
        processed.append(consolidate_instance_vcpus(sample))
    processed.append(process_raw(sample))
    return processed

# FIXME: we want to make sourcedict inclusion whitelist-based rather
# than blacklist-based, so we know what we're getting.
def remove_extraneous(sourcedict):
    """
    Remove the keys we will never care about and/or change with each
    message. This should be the last thing done to a sourcedict.
    """
    for k in KEYS_TO_DELETE:
        sourcedict.pop(k, None)

def process_raw(sample):
    """
    Process a sample into a raw (not consolidated) datapoint. Makes
    destructive updates to sample.
    """
    event_type = sample["resource_metadata"].get("event_type", None)
    display_name = sample["resource_metadata"].get("display_name", None)

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

    if display_name is not None:
        sourcedict["display_name"] = display_name
    if event_type is not None:
        sourcedict["event_type"] = event_type

    sourcedict["metric_name"] = sanitize(sourcedict.pop("name"))
    sourcedict["metric_type"] = sanitize(sourcedict.pop("type"))

    remove_extraneous(sourcedict)

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

def consolidate_instance_flavor(sample):
    name = "instance_flavor"
    payload = hash_flavor_id(sample["resource_metadata"]["instance_type"])
    timestamp = sanitize_timestamp(sample["timestamp"])
    sourcedict = get_base_sourcedict(payload, sample, name, consolidated=True)
    address = get_address(sample, name, consolidated=True)
    return (address, sourcedict, timestamp, payload)

# FIXME: tests
def consolidate_instance_vcpus(sample):
    name = "instance_vcpus"
    payload = sample["resource_metadata"]["flavor"]["vcpus"]
    timestamp = sanitize_timestamp(sample["timestamp"])
    sourcedict = get_base_sourcedict(payload, sample, name, consolidated=True)
    address = get_address(sample, name, consolidated=True)
    return (address, sourcedict, timestamp, payload)

# FIXME: tests
def consolidate_instance_ram(sample):
    name = "instance_ram"
    payload = sample["resource_metadata"]["flavor"]["ram"]
    timestamp = sanitize_timestamp(sample["timestamp"])
    sourcedict = get_base_sourcedict(payload, sample, name, consolidated=True)
    address = get_address(sample, name, consolidated=True)
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

def consolidate_instance_event(sample):
    metadata     = sample["resource_metadata"]
    payload = get_consolidated_payload(metadata["event_type"], metadata.get("message",""), metadata["instance_type_id"])
    return process_consolidated_event(sample, payload)

def consolidate_volume_event(sample):
    metadata     = sample["resource_metadata"]
    payload = get_consolidated_payload(metadata["event_type"], metadata.get("status",""), sample["volume"])
    return process_consolidated_event(sample, payload)

def consolidate_ip_event(sample):
    metadata     = sample["resource_metadata"]
    payload = get_consolidated_payload(metadata["event_type"], "", RAW_PAYLOAD_IP_ALLOC)
    return process_consolidated_event(sample, payload)

def process_consolidated_event(sample, payload):
    name         = sample["name"]
    metadata     = sample["resource_metadata"]
    timestamp    = sanitize_timestamp(sample["timestamp"])
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

    if message.lower() == "success":
        eventResolution = 0
    # Empty message implies success
    elif message == "":
        eventResolution = 0
    elif message.lower() == "failure":
        eventResolution = 1
    elif message.lower() == "error":
        eventResolution = 2

    rest,maybeVerb = event_type.rsplit('.', 1)
    maybeVerb = maybeVerb.lower()
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
