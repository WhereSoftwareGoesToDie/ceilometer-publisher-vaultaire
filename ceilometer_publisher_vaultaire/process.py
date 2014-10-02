import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc

from marquise import Marquise

import siphash
import ceilometer_publisher_vaultaire.consolidated as consolidated

KEYS_TO_DELETE = [
    "timestamp",
    "volume",
    "created_at",
    "updated_at",
    "id",
    "size",
]

# FIXME: refactor the functions in this module in such a way that they
# can log.

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

    id_elements = [
        sample["resource_id"],
        sample["project_id"],
        sample["name"],
        sample["unit"],
        event_type,
        flavor_type,
    ]

    # Filter out Nones and stringify everything so we don't get TypeErrors on concatenation
    id_elements = [ str(x) for x in id_elements if x is not None ]

    # Generate the unique identifer for the sample
    identifier = "".join(id_elements)
    address = Marquise.hash_identifier(identifier)

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
        payload = canonical_siphash(metadata["instance_type"])
    elif name.startswith("volume.size"):
        payload = consolidated.volumeToRawPayload(sanitize(sample["volume"]))
    elif name.startswith("ip.floating"):
        payload = consolidated.RAW_PAYLOAD_IP_ALLOC
    else:
        payload = sanitize(sample["volume"])

    # Build the source dict
    sourcedict = {}
    sourcedict["_consolidated"] = 1
    sourcedict["project_id"]    = project_id
    sourcedict["resource_id"]   = resource_id
    sourcedict["counter_name"]  = name
    sourcedict["counter_unit"]  = counter_unit
    sourcedict["counter_type"]  = counter_type
    sourcedict["display_name"]  = metadata.get("display_name", "")
    # Vaultaire does not care about the datatype of the payload, so we
    # need to specify it in metadata.
    if type(payload) == float:
        sourcedict["_float"] = 1

    # If it's a cumulative value, this needs to go in the metadata as
    # well.
    if counter_type == "cumulative":
        sourcedict["_counter"] = 1

    for k, v in sourcedict.items():
        sourcedict[k] = sanitize(str(v))

    # Common elements to all messages are r_id + p_id + counter_(name,
    # type, unit)
    id_elements = [
        resource_id,
        project_id,
        name,
        counter_type,
        counter_unit,
        "_consolidated",
    ]

    # Filter out Nones and stringify everything so we don't get
    # TypeErrors on concatenation
    id_elements = [ str(x) for x in id_elements if x is not None ]

    # Generate the unique identifer for the sample
    identifier = "".join(id_elements)
    address = Marquise.hash_identifier(identifier)
    return (address, sourcedict, timestamp, payload)

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
        payload = consolidated.constructPayload(metadata["event_type"], metadata.get("message",""), metadata["instance_type_id"])
    elif name.startswith("volume.size"):
        payload = consolidated.constructPayload(metadata["event_type"], metadata.get("status",""), consolidated.volumeToRawPayload(sample["volume"]))
    elif name.startswith("ip.floating"):
        payload = consolidated.constructPayload(metadata["event_type"], "", 1)
    else:
        payload = sanitize(sample["volume"])

    # Build the source dict
    sourcedict = {}
    sourcedict["_event"]        = 1
    sourcedict["_consolidated"] = 1
    sourcedict["project_id"]    = project_id
    sourcedict["resource_id"]   = resource_id
    sourcedict["counter_name"]  = name
    sourcedict["counter_unit"]  = counter_unit
    sourcedict["counter_type"]  = counter_type
    sourcedict["display_name"]  = metadata.get("display_name", "")
    ## Vaultaire cares about the datatype of the payload
    if type(payload) == float:
        sourcedict["_float"] = 1

    ## If it's a cumulative value, we need to tell vaultaire
    if counter_type == "cumulative":
        sourcedict["_counter"] = 1

    for k, v in sourcedict.items():
        sourcedict[k] = sanitize(str(v))

    # Common elements to all messages are r_id + p_id + counter_(name,
    # type, unit)
    id_elements = [
        resource_id,
        project_id,
        name,
        counter_type,
        counter_unit,
        "_event",
        "_consolidated",
    ]

    # Filter out Nones and stringify everything so we don't get
    # TypeErrors on concatenation
    id_elements = [ str(x) for x in id_elements if x is not None ]

    # Generate the unique identifer for the sample
    identifier = "".join(id_elements)
    address = Marquise.hash_identifier(identifier)
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

def canonical_siphash(the_thing):
    return siphash.SipHash24("0000000000000000", the_thing).hash()

def get_flavor_type(sample):
    flavor_type = None
    if "flavor" in sample:
        flavor_type = sample["flavor"].get("name", None)
    elif "instance_type" in sample:
        flavor_type = sample["instance_type"]
    return flavor_type
