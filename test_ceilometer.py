"""We can't do much real ceilometer work, but we can test the functional
bits of our code.
"""

import sys
import json
import pprint
from hashlib import sha1
from collections import OrderedDict as OD
import ceilometer_publisher_vaultaire

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

def test_ODrepr():
    """Force an OrderedDict through our customer __repr__ function to keep
    the coverage tests happy."""
    parsed_json = json.JSONDecoder(object_pairs_hook=OD).decode(sample_json)
    _ = PF(parsed_json)


def test_process_sample():
    raise RuntimeError("Test not yet written")

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
    parsed_json = json.JSONDecoder(object_pairs_hook=OD).decode(sample_json)
    ceilometer_publisher_vaultaire.process._remove_extraneous(parsed_json)
    trimmed_json = json.dumps(parsed_json)
    expected_json = json.dumps(json.JSONDecoder(object_pairs_hook=OD).decode(expected))
    assert trimmed_json == expected_json


def test_process_raw():

    raise RuntimeError("Test not yet written")


def test_process_consolidated():
    raise RuntimeError("Test not yet written")


def test_sanitize():
    raise RuntimeError("Test not yet written")


def test_flatten():
    """Take a known-good input, flatten it, then compare the result with a
    known-good version that we've flattened beforehand. Results should match.
    """

    # We need to jump through a few hoops to ensure consistent iteration order
    # over our data structure. So instead of using a dict, we use an
    # OrderedDict.
    #
    # http://stackoverflow.com/questions/6921699/can-i-get-json-to-load-into-an-ordereddict-in-python
    parsed_json = json.JSONDecoder(object_pairs_hook=OD).decode(sample_json)
    flattened_json = ceilometer_publisher_vaultaire.flatten(parsed_json)

    normalised_dump = """{"id": "a994b3b8-21c6-11e4-8d23-a60a09774fec", "name": "disk.read.requests", "project_id": "78e2fc2a70314713b9f814ec634b5e10", "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d", "resource_metadata": {"display_name": "bar2", "flavor": {"disk": 0, "ephemeral": 0, "id": "42", "links": [{"href": "http://192.168.42.114:8774/0632438bb4a748218b43ed9223b39f77/flavors/42", "rel": "bookmark"}], "name": "m1.nano", "ram": 64, "vcpus": 1}, "host": "2a83dab20754db95ed7cf5a02ee0056d9d0f3d033215eb634403c533", "instance_type": "42", "memory_mb": 64, "name": "instance-00000002", "status": "active", "vcpus": 1}, "timestamp": "2014-08-12T02:16:26Z", "type": "cumulative", "unit": "request", "user_id": "8288e6719e8c4b6a8b130ae77af7abbc", "volume": 332}"""
    flattened_dump = """{"id": "a994b3b8-21c6-11e4-8d23-a60a09774fec", "name": "disk.read.requests", "project_id": "78e2fc2a70314713b9f814ec634b5e10", "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d", "resource_metadata-display_name": "bar2", "resource_metadata-flavor-disk": 0, "resource_metadata-flavor-ephemeral": 0, "resource_metadata-flavor-id": "42", "resource_metadata-flavor-name": "m1.nano", "resource_metadata-flavor-ram": 64, "resource_metadata-flavor-vcpus": 0, "resource_metadata-host": "2a83dab20754db95ed7cf5a02ee0056d9d0f3d033215eb634403c533", "resource_metadata-instance_type": "42", "resource_metadata-memory_mb": 64, "resource_metadata-name": "instance-00000002", "resource_metadata-status": "active", "resource_metadata-vcpus": 0, "timestamp": "2014-08-12T02-16-26Z", "type": "cumulative", "unit": "request", "user_id": "8288e6719e8c4b6a8b130ae77af7abbc", "volume": 332}"""

    assert json.dumps(parsed_json).encode('utf8')    == normalised_dump
    assert json.dumps(flattened_json).encode('utf8') == flattened_dump
    print("NormSample JSON hashes to:    {}".format(sha1(json.dumps(parsed_json).encode('utf8')).hexdigest()))
    print("Flattened JSON hashes to:     {}".format(sha1(json.dumps(flattened_json).encode('utf8')).hexdigest()))


def test_sanitize_timestamp():
    raise RuntimeError("Test not yet written")

if __name__ == '__main__':
    test_ODrepr()

    # process.py
    test_process_sample()
    test__remove_extraneous()
    test_process_raw()
    test_process_consolidated()
    test_sanitize()
    test_flatten()
    test_sanitize_timestamp()

    sys.exit(0)
