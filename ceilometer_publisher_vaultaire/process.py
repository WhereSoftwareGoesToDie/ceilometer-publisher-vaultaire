import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc

from marquise import Marquise

import ceilometer_publisher_vaultaire.siphash

### Top Level

def process_sample(sample):
    processed = []
    name = sample["name"]
    # We want to do some more processing to these event types...
    if name.startswith("instance") and not is_event_sample(sample):
        processed.append(consolidate_instance_flavor(sample))
        processed.append(consolidate_instance_ram(sample))
        processed.append(consolidate_instance_vcpus(sample))
        processed.append(consolidate_instance_disk(sample))
    elif name == "cpu" and not is_event_sample(sample):
        processed.append(consolidate_cpu(sample))
    elif (name == "disk.write.bytes" or name == "disk.read.bytes") \
            and not is_event_sample(sample):
        processed.append(consolidate_diskio(sample))
    elif (name == "network.incoming.bytes" or name == "network.outgoing.bytes") \
            and not is_event_sample(sample):
        processed.append(consolidate_network(sample))
    elif name == "ip.floating" and is_event_sample(sample):
        processed.append(consolidate_ip_event(sample))
    elif name == "volume.size" and is_event_sample(sample):
        processed.append(consolidate_volume_event(sample))
    return processed

### Common Generation

def is_event_sample(sample):
    return "event_type" in sample["resource_metadata"]

def get_base_sourcedict(payload, sample, name):
    sourcedict = {}
    sourcedict["_event"]        = 1 if is_event_sample(sample) else 0
    sourcedict["_consolidated"] = 1
    sourcedict["project_id"]    = sample["project_id"]
    sourcedict["resource_id"]   = sample["resource_id"]
    sourcedict["metric_name"]   = name
    sourcedict["counter_unit"]  = sample["unit"]
    sourcedict["metric_type"]   = sample["type"]
    sourcedict["display_name"]  = sample["resource_metadata"].get("display_name", "")

    if sample["type"] == "cumulative":
        sourcedict["_counter"] = 1

    if type(payload) == float:
        sourcedict["_float"] = 1

    for k, v in sourcedict.items():
        sourcedict[k] = sanitize(str(v))

    return sourcedict

def get_id_elements(sample, name):
    """Return a list of components uniquely identifying a metric."""
    id_elements = [
        sample["project_id"],
        sample["resource_id"],
        sample["unit"],
        sample["type"],
        name,
        "_consolidated"
    ]
    # If this is an event sample, the event type is always part of the
    # identifier (as is the fact that it is an event).
    if is_event_sample(sample):
        id_elements.append("_event")
        id_elements.append(sample["resource_metadata"]["event_type"])
    return id_elements

def get_address(sample, name):
    """
    Return a unique Vaultaire address calculated for the metric.

    The address is calculated based on the metric's "id_elements", which
    are considered to be the list of strings forming the primary key for
    the relevant metric.
    """
    id_elements = get_id_elements(sample, name)
    id_elements = [ str(x) for x in id_elements if x is not None ]
    identifier = "".join(id_elements)
    return Marquise.hash_identifier(identifier)

def get_core_triple(payload, sample, name):
    """
    Return the (address, sourcedict, timestamp) triple of the given
    (payload, sample, name) triple
    """
    address = get_address(sample, name)
    sourcedict = get_base_sourcedict(payload, sample, name)
    timestamp = sanitize_timestamp(sample["timestamp"])
    return (address, sourcedict, timestamp)

### Consolidated Pollsters

def process_base_pollster(sample):
    """The standard process for converting a pollster sample into a 4-tuple"""
    name = sample["name"]
    payload = sample["volume"]
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_cpu(sample):
    return process_base_pollster(sample)

def consolidate_diskio(sample):
    return process_base_pollster(sample)

def consolidate_network(sample):
    return process_base_pollster(sample)

def consolidate_instance_vcpus(sample):
    name = "instance_vcpus"
    payload = sample["resource_metadata"]["flavor"]["vcpus"]
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_instance_ram(sample):
    name = "instance_ram"
    payload = sample["resource_metadata"]["flavor"]["ram"]
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_instance_disk(sample):
    name = "instance_disk"
    payload = sample["resource_metadata"]["flavor"]["disk"] + \
              sample["resource_metadata"]["flavor"]["ephemeral"]
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_instance_flavor(sample):
    name = "instance_flavor"
    payload = hash_flavor_id(sample["resource_metadata"]["instance_type"])
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def hash_flavor_id(flavor_id):
    """
    Return the SipHash-2-4 of a string, using the null key.

    We do this to instance types before we store them because a)
    instance types are strings and b) we can't assume they won't change
    (ceilometer doesn't give us easy access to the guaranteed-integral
    instance_type_id).
    """
    return ceilometer_publisher_vaultaire.siphash.SipHash24("\0"*16, flavor_id).hash()

### Consolidated Events

RAW_PAYLOAD_IP_ALLOC = 1

def consolidate_volume_event(sample):
    name = "volume.size"
    payload  = get_volume_payload(sample)
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_ip_event(sample):
    name = "ip.floating"
    payload = get_ip_payload(sample)
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def get_volume_payload(sample):
    split_event_type = sample["resource_metadata"]["event_type"].split('.')
    status = sample["resource_metadata"]["status"]
    verb = split_event_type[1]
    endpoint = split_event_type[2]
    raw_payload = sample["volume"]

    if status == "available":
        status_value = 1
    elif status == "creating":
        status_value = 2
    elif status == "extending":
        status_value = 3
    elif status == "deleting":
        status_value = 4

    else:
        raise Exception("Unexpected status: " + status + " for volume event")

    if verb == "create":
        verb_value = 1
    elif verb == "resize":
        verb_value = 2
    elif verb == "delete":
        verb_value = 3
    else:
        raise Exception("Unexpected verb: " + verb + " for volume event")

    if endpoint == "start":
        endpoint_value = 1
    elif endpoint == "end":
        endpoint_value = 2
    else:
        raise Exception("Unexpected endpoint: " + endpoint + " for volume event")

    return construct_consolidated_event_payload(status_value, verb_value, endpoint_value, raw_payload)

def get_ip_payload(sample):
    split_event_type = sample["resource_metadata"]["event_type"].split('.')
    status = sample["resource_metadata"]["status"]
    verb = split_event_type[1]
    endpoint = split_event_type[2]
    raw_payload = RAW_PAYLOAD_IP_ALLOC

    if status == "ACTIVE":
        status_value = 1
    elif status == "DOWN":
        status_value = 2
    else:
        raise Exception("Unexpected status: " + status + " for ip.floating event")

    if verb == "create":
        verb_value = 1
    elif verb == "update":
        verb_value = 2
    else:
        raise Exception("Unexpected verb: " + verb + " for ip.floating event")

    if endpoint == "start":
        endpoint_value = 1
    elif endpoint == "end":
        endpoint_value = 2
    else:
        raise Exception("Unexpected endpoint: " + endpoint + " for ip.floating event")

    return construct_consolidated_event_payload(status_value, verb_value, endpoint_value, raw_payload)

def construct_consolidated_event_payload(status_value, verb_value, endpoint_value, raw_payload):
    return status_value + (verb_value << 8) + (endpoint_value << 16) + (raw_payload << 32)

### Sanitization

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




