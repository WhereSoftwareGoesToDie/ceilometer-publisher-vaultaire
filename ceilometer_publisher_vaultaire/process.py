"""
The core module for processing ceilometer samples for use with
vaultaire and marquise. process_sample is the core function
which should be used from this module
"""

import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc

from marquise import Marquise

import ceilometer_publisher_vaultaire.siphash

### Top Level

def process_sample(sample):
    """
    Given a ceilometer sample as a dict, processes into a list of
    (address, sourcedict, timestamp, payload) 4-tuples
    For instance pollsters this will be a list of 4 items, one each
    for flavor, ram, vcpus and disk. For all other supported metrics
    process_sample will return a singleton list.
    """
    processed = []
    name = sample["name"]
    # We want to do some more processing to these event types...
    if name == "instance" and not is_event_sample(sample):
        processed.append(consolidate_instance_flavor(sample))
        processed.append(consolidate_instance_ram(sample))
        processed.append(consolidate_instance_vcpus(sample))
        processed.append(consolidate_instance_disk(sample))
    elif name == "cpu" and not is_event_sample(sample):
        processed.append(process_base_pollster(sample))
    elif (name == "disk.write.bytes" or name == "disk.read.bytes") \
            and not is_event_sample(sample):
        processed.append(process_base_pollster(sample))
    elif (name == "network.incoming.bytes" or name == "network.outgoing.bytes") \
            and not is_event_sample(sample):
        processed.append(process_base_pollster(sample))
    elif name == "ip.floating" and is_event_sample(sample):
        processed.append(consolidate_ip_event(sample))
    elif name == "volume.size" and is_event_sample(sample):
        processed.append(consolidate_volume_event(sample))
    return processed

### Common Generation

def is_event_sample(sample):
    """Returns true given an event sample, false given a pollster sample"""
    return "event_type" in sample["resource_metadata"]

def get_base_sourcedict(payload, sample, name):
    """Generates a sourcedict from the given payload, sample and name"""
    sourcedict = {}
    sourcedict["_event"]        = 1 if is_event_sample(sample) else 0
    sourcedict["_consolidated"] = 1
    sourcedict["project_id"]    = sample["project_id"]
    sourcedict["resource_id"]   = sample["resource_id"]
    sourcedict["metric_name"]   = name
    sourcedict["metric_unit"]   = sample["unit"]
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

def consolidate_instance_vcpus(sample):
    """Produces a vcpu 4-tuple from an instance pollster sample"""
    name = "instance_vcpus"
    payload = sample["resource_metadata"]["flavor"]["vcpus"]
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_instance_ram(sample):
    """Produces a ram 4-tuple from an instance pollster sample"""
    name = "instance_ram"
    payload = sample["resource_metadata"]["flavor"]["ram"]
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_instance_disk(sample):
    """
    Produces a disk 4-tuple from an instance pollster sample
    Disk is split up into root (disk) and ephemeral on openstack
    So we sum them to get the total value
    """
    name = "instance_disk"
    payload = sample["resource_metadata"]["flavor"]["disk"] + \
              sample["resource_metadata"]["flavor"]["ephemeral"]
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_instance_flavor(sample):
    """
    Produces an instance_flavor 4-tuple from an instance pollster sample
    Uses the SipHash-2-4 using the null key of the "instance_type" field
    in the resource_metadata of the sample. We do this  because:
    a) instance_types are strings and
    b) we can't assume they won't change (ceilometer doesn't give us easy
    access to the guaranteed-integral instance_type_id).
    """
    name = "instance_flavor"
    payload = hash_flavor_id(sample["resource_metadata"]["instance_type"])
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def hash_flavor_id(flavor_id):
    """Return the SipHash-2-4 of a string, using the null key."""
    return ceilometer_publisher_vaultaire.siphash.SipHash24("\0"*16, flavor_id).hash()

### Consolidated Events

RAW_PAYLOAD_IP_ALLOC = 1

def consolidate_volume_event(sample):
    """Produces a volume 4-tuple from a volume.size event sample"""
    name = "volume.size"
    payload  = get_volume_payload(sample)
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def consolidate_ip_event(sample):
    """Produces an ip allocation 4-tuple from an ip.floating event sample"""
    name = "ip.floating"
    payload = get_ip_payload(sample)
    (address, sourcedict, timestamp) = get_core_triple(payload, sample, name)
    return (address, sourcedict, timestamp, payload)

def get_volume_payload(sample):
    """Returns the volume compound consolidated payload for the given sample"""
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
    """Returns the ip allocation compound consolidated payload for the given sample"""
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
    """
    Constructs an 8 byte compound consolidated payload from the raw payload and
    then given enum values for status, verb and endpoint
    Format from least significant to most significant bytes:
    Byte    0: Status
    Byte    1: Verb
    Byte    2: Endpoint
    Byte    3: Reserved
    Bytes 4-7: Raw Payload
    """
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
    NANOSECONDS_PER_MICROSECOND = 10**3
    if type(v) is datetime.datetime:
        timestamp = v
    else: # We have a string.
        timestamp = parse(v)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=tzutc())
    # If we get here, we've successfully grabbed a datetime.
    # FIXME: use strftime
    epoch = datetime.datetime(1970, 1, 1, tzinfo=tzutc())
    td = timestamp - epoch
    micro_since_epoch = td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6
    return int(micro_since_epoch * NANOSECONDS_PER_MICROSECOND)
