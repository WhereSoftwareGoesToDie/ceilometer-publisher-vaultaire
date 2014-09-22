#!/bin/usr/mapythsz

from pprint import pformat
import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc

from marquise import Marquise

import payload as p

def process_sample(sample):
    sample = sample.as_dict()
    name = sample["name"]
    metadata = sample["resource_metadata"]

    # Generate the unique identifer for the sample
    ## CPU
    cpu_number = sample.get("cpu_number", "")
    ## Events
    event_type = metadata.get("event_type", "")
    ### If this is event data we care about timestamp + message (Success/Failure)
    timestamp = ""
    message = ""
    if event_type != "":
        timestamp = sample["timestamp"]
        message = sample["resource_metadata"].get("message", "")

    ## Instance related things
    flavor_type = ""
    ### If the flavor key is present, the value of instance_type is really instance_type_id
    if "flavor" in sample:
        flavor_type = sample["flavor"].get("name", "")
    elif "instance_type" in sample:
        flavor_type = sample["instance_type"]
    ## Common = r_id + p_id + counter_(name, type, unit)
    identifier = sample["resource_id"] + sample["project_id"] + \
                 name + sample["type"] + sample["unit"] + \
                 flavor_type + cpu_number + message + timestamp
    address = Marquise.hash_identifier(identifier)

    #We always care about the project and resource IDs
    sourcedict = {}
    sourcedict["project_id"] = sample["project_id"]
    sourcedict["resource_id"] = sample["resource_id"]
    sourcedict["counter_name"] = name
    # Cast unit as a special metadata type
    sourcedict["uom"] = sanitize(sample["unit"])
    sourcedict["counter_type"] = sample["type"]


    # Our payload is the volume (later parsed to "counter_volume" in ceilometer)
    payload = sanitize(sample["volume"])
    # Sanitize timestamp (will parse timestamp to nanoseconds since epoch)
    timestamp = sanitize(sample["timestamp"])

    # Add specific things per meter
    if name == "cpu":
        sourcedict["cpu_number"] = metadata["cpu_number"]
    elif name == "instance":
        payload = p.constructPayload(metadata["event_type"], metadata["message"], p.instanceToRawPayload(metadata["instance_type"]))
    # Vaultaire cares about the datatype of the payload
    if type(payload) == float:
        sourcedict["_float"] = 1
    elif type(payload) == str:
        sourcedict["_extended"] = 1

    # If it's a cumulative value, we need to tell vaultaire
    if sample["type"] == "cumulative":
        sourcedict["_counter"] = 1

    return (address, sourcedict, timestamp, payload)

def sanitize(v):
    """Sanitize a value into something that Marquise can use. This weeds
    out None keys/values, and ensures that timestamps are consistently
    formatted.
    """
    if v is None:
        return ""
    if v in (True,False):
        return 1 if v is True else 0
    if type(v) is unicode:
        v = str(v)
    try:
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
        epoch = datetime.datetime(1970, 1, 1, tzinfo=tzutc())
        time_since_epoch = (timestamp - epoch).total_seconds() # total_seconds() is in Py2.7 and later.
        return int(time_since_epoch * NANOSECONDS_PER_SECOND)
    except (ValueError,AttributeError): # ValueError for bad strings, AttributeError for bad input type.
        # If parsing fails then assume it's not a valid datestamp/timestamp.
        # Instead, treat it as a primitive type and stringify it accordingly.
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
        if prefix:
            k = "{}-{}".format(prefix,k)

        # This was previously a check for __iter__, but strings have those now,
        # so let's just check for dict-ness instead. No good on lists anyway.
        if type(v) is not dict:
            v = sanitize(v)
            flattened_dict[k] = v
        else:
            flattened_dict.update(flatten(v, k))
    return flattened_dict
