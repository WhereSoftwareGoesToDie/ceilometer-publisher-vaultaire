"""We can't do much real ceilometer work, but we can test the functional
bits of our code.
"""

import sys
import json
import pprint
from hashlib import sha1
from collections import OrderedDict as OD
import ceilometer_publisher_vaultaire
import copy as copy

PP = pprint.pprint
PF = pprint.pformat

# This allows OrderedDicts to be pretty-printed (at least somewhat prettier).
def ODprint(x):
    return "\n\t".join( ["{"] + [ "{}: {}".format(k,v) for (k,v) in x.iteritems() ] + ["}\n"] )
OD.__repr__ = ODprint


# This sample is arbitrarily trimmed down and is probably missing keys.
sample_json = """
                {
                    "id": "a994b3b8-21c6-11e4-8d23-a60a09774fec",
                    "name": "disk.read.requests",
                    "project_id": "78e2fc2a70314713b9f814ec634b5e10",
                    "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d",
                    "resource_metadata": {
                        "display_name": "bar2",
                        "flavor": {
                            "disk": 0,
                            "ephemeral": 0,
                            "id": "42",
                            "links": [
                                {
                                    "href": "http://192.168.42.114:8774/0632438bb4a748218b43ed9223b39f77/flavors/42",
                                    "rel": "bookmark"
                                }
                            ],
                            "name": "m1.nano",
                            "ram": 64,
                            "vcpus": 1
                        },
                        "host": "2a83dab20754db95ed7cf5a02ee0056d9d0f3d033215eb634403c533",
                        "instance_type": "42",
                        "memory_mb": 64,
                        "name": "instance-00000002",
                        "status": "active",
                        "vcpus": 1
                    },
                    "timestamp": "2014-08-12T02:16:26Z",
                    "type": "cumulative",
                    "unit": "request",
                    "user_id": "8288e6719e8c4b6a8b130ae77af7abbc",
                    "volume": 332
                }
"""

event_json = """
                {
                    "project_id": "123",
                    "resource_id": "456",
                    "name": "instance",
                    "type": "gauge",
                    "unit": "instance",
                    "volume": 23,
                    "timestamp": "1970-01-01 00:00:00",
                    "resource_metadata": {
                    "display_name": "bob",
                        "event_type": "instance.create.end",
                        "message": "Success"
                    },
                    "flavor": {
                        "name": "m1.tiny"
                    }
                }
                """

mini_json = """
                {
                    "project_id": "123",
                    "resource_id": "456",
                    "name": "instance",
                    "type": "gauge",
                    "unit": "instance",
                    "volume": 23,
                    "display_name": "bob",
                    "timestamp": "1970-01-01 00:00:00",
                    "resource_metadata": {
                        "foo": "bar"
                    }
                }
                """
parsed_json = json.loads(sample_json)
parsed_event_json = json.loads(event_json)
parsed_mini_json = json.loads(mini_json)

def test_process_sample():
    mini_list = ceilometer_publisher_vaultaire.process.process_sample(copy.copy(parsed_mini_json))
    event_list = ceilometer_publisher_vaultaire.process.process_sample(copy.copy(parsed_event_json))
    #check that the for the non-event only raw was called
    assert len(mini_list) == 2
    #check the 
    assert len(event_list) == 2
    #Check the addresses the raw and consolidated versions produce are distinct
    assert event_list[0][0] != event_list[1][0]
    #Check the addresses the raw version produces are distinct across event/non-event
    assert mini_list[0][0] != event_list[1][0]

def test__remove_extraneous():
    expected = """
                {
                    "name": "disk.read.requests",
                    "project_id": "78e2fc2a70314713b9f814ec634b5e10",
                    "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d",
                    "resource_metadata": {
                        "display_name": "bar2",
                        "flavor": {
                            "disk": 0,
                            "ephemeral": 0,
                            "id": "42",
                            "links": [
                                {
                                    "href": "http://192.168.42.114:8774/0632438bb4a748218b43ed9223b39f77/flavors/42",
                                    "rel": "bookmark"
                                }
                            ],
                            "name": "m1.nano",
                            "ram": 64,
                            "vcpus": 1
                        },
                        "host": "2a83dab20754db95ed7cf5a02ee0056d9d0f3d033215eb634403c533",
                        "instance_type": "42",
                        "memory_mb": 64,
                        "name": "instance-00000002",
                        "status": "active",
                        "vcpus": 1
                    },
                    "type": "cumulative",
                    "unit": "request",
                    "user_id": "8288e6719e8c4b6a8b130ae77af7abbc"
                }
"""
    local_json = copy.copy(parsed_json)
    ceilometer_publisher_vaultaire.process._remove_extraneous(local_json)
    expected_json = json.loads(expected)
    assert local_json == expected_json


def test_process_raw():
    process_raw = ceilometer_publisher_vaultaire.process.process_raw

    expectedMiniSd = {"project_id": "123", "resource_id": "456", "counter_name": "instance", "counter_type": "gauge", "_unit": "instance", "display_name": "bob", "foo": "bar"}
    expectedEventSd = {"project_id": "123", "resource_id": "456", "counter_name": "instance", "counter_type": "gauge", "_unit": "instance", "display_name": "bob", "event_type": "instance.create.end", "flavor-name": "m1.tiny", "message": "Success"}
    (addr1, sd1, ts1, p1) = process_raw(copy.copy(parsed_mini_json))
    (addr2, sd2, ts2, p2) = process_raw(copy.copy(parsed_event_json))
    assert sd1 == expectedMiniSd
    assert sd2 == expectedEventSd
    assert sd1 != sd2
    assert ts1 == 0
    assert ts1 == ts2
    assert p1 == 23
    assert p1 == p2
    assert addr1 != addr2

def test_process_consolidated_event():

    parsed_event_json = json.loads(event_json)
    (_, sd, ts, p) = ceilometer_publisher_vaultaire.process_consolidated_event(parsed_event_json)
    expected_sd =  {"project_id": "123", "resource_id": "456", "counter_name": "instance", "counter_type": "gauge", "counter_unit": "instance", "_consolidated": "1", "_event": "1", "display_name": "bob"}
    assert sd == expected_sd
    assert p == (1 << 8 + 2 << 16 + 1 << 32)
    assert ts == 0


def test_sanitize():
    sanitize = ceilometer_publisher_vaultaire.process.sanitize
    assert sanitize(None)  == ""
    assert sanitize(True)  == 1
    assert sanitize(False) == 0
    assert sanitize("maryhadalittlelamb") == "maryhadalittlelamb"
    assert sanitize("\"numberOf****sGiven\":\"0\"") == "\"numberOf****sGiven\"-\"0\""
    assert sanitize("[this,is,a,list]") == "[this-is-a-list]"

def test_flatten():
    """Take a known-good input, flatten it, then compare the result with a
    known-good version that we've flattened beforehand. Results should match.
    """

    # We need to jump through a few hoops to ensure consistent iteration order
    # over our data structure. So instead of using a dict, we use an
    # OrderedDict.
    #
    # http://stackoverflow.com/questions/6921699/can-i-get-json-to-load-into-an-ordereddict-in-python
    flattened_json = ceilometer_publisher_vaultaire.flatten(copy.copy(parsed_json))

    normalised_dump = """{"id": "a994b3b8-21c6-11e4-8d23-a60a09774fec", "name": "disk.read.requests", "project_id": "78e2fc2a70314713b9f814ec634b5e10", "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d", "resource_metadata": {"display_name": "bar2", "flavor": {"disk": 0, "ephemeral": 0, "id": "42", "links": [{"href": "http://192.168.42.114:8774/0632438bb4a748218b43ed9223b39f77/flavors/42", "rel": "bookmark"}], "name": "m1.nano", "ram": 64, "vcpus": 1}, "host": "2a83dab20754db95ed7cf5a02ee0056d9d0f3d033215eb634403c533", "instance_type": "42", "memory_mb": 64, "name": "instance-00000002", "status": "active", "vcpus": 1}, "timestamp": "2014-08-12T02:16:26Z", "type": "cumulative", "unit": "request", "user_id": "8288e6719e8c4b6a8b130ae77af7abbc", "volume": 332}"""
    flattened_dump = """{"id": "a994b3b8-21c6-11e4-8d23-a60a09774fec", "name": "disk.read.requests", "project_id": "78e2fc2a70314713b9f814ec634b5e10", "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d", "resource_metadata-display_name": "bar2", "resource_metadata-flavor-disk": 0, "resource_metadata-flavor-ephemeral": 0, "resource_metadata-flavor-id": "42", "resource_metadata-flavor-name": "m1.nano", "resource_metadata-flavor-ram": 64, "resource_metadata-flavor-vcpus": 1, "resource_metadata-host": "2a83dab20754db95ed7cf5a02ee0056d9d0f3d033215eb634403c533", "resource_metadata-instance_type": "42", "resource_metadata-memory_mb": 64, "resource_metadata-name": "instance-00000002", "resource_metadata-status": "active", "resource_metadata-vcpus": 1, "timestamp": "2014-08-12T02-16-26Z", "type": "cumulative", "unit": "request", "user_id": "8288e6719e8c4b6a8b130ae77af7abbc", "volume": 332}"""

    assert parsed_json == json.loads(normalised_dump)
    assert flattened_json == json.loads(flattened_dump)
    print("NormSample JSON hashes to:    {}".format(sha1(json.dumps(parsed_json).encode('utf8')).hexdigest()))
    print("Flattened JSON hashes to:     {}".format(sha1(json.dumps(flattened_json).encode('utf8')).hexdigest()))


def test_sanitize_timestamp():
    """pylint sucks donkey word"""
    sanitize_timestamp = ceilometer_publisher_vaultaire.process.sanitize_timestamp
    assert sanitize_timestamp("1970-01-01 00:00:00") == 0
    assert sanitize_timestamp("1970-01-01T00:00:00Z") == 0
    assert sanitize_timestamp("1970-01-01 00:00:00-0200") == 7200*10**9
    assert sanitize_timestamp("1993-03-17T21:00:00+1000") == 732366000*10**9

if __name__ == '__main__':

    # process.py
    test_process_sample()
    test__remove_extraneous()
    test_process_raw()
    test_process_consolidated_event()
    test_sanitize()
    test_flatten()
    test_sanitize_timestamp()

    sys.exit(0)
