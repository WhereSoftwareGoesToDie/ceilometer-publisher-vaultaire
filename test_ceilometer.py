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
import ceilometer_publisher_vaultaire.siphash as siphash

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

instance_pollster_json = """
{
    "id": "90b350ac-4a07-11e4-a84e-ecf4bbc1fce4",
    "name": "instance:m1.small",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "70979ae8-abc8-42dd-856e-19016911f615",
    "resource_metadata": {
        "OS-EXT-AZ:availability_zone": "syd1",
        "disk_gb": 20,
        "display_name": "testcustomer-lamed-70979ae8-abc8-42dd-856e-19016911f615",
        "ephemeral_gb": 0,
        "flavor": {
            "disk": 20,
            "ephemeral": 0,
            "id": "2",
            "links": [
                {
                    "href": "http://nova.int.syd1.prod.os.anchor.net.au:8774/bd3f518204b54407854a7dd8d1c49931/flavors/2",
                    "rel": "bookmark"
                }
            ],
            "name": "m1.small",
            "ram": 2048,
            "vcpus": 1
        },
        "host": "99b3b0658f1b29714aa27bc509e4ab82b19a524d1dc3f6b04ac53e42",
        "image": null,
        "image_ref": null,
        "image_ref_url": null,
        "instance_type": "2",
        "kernel_id": null,
        "memory_mb": 2048,
        "name": "instance-000009c9",
        "ramdisk_id": null,
        "root_gb": 20,
        "vcpus": 1
    },
    "source": "openstack",
    "timestamp": "2014-10-02T07:41:48Z",
    "type": "gauge",
    "unit": "instance",
    "user_id": "1f6f57489d404fe293c65d19e9b31df2",
    "volume": 1
}
"""

expected_consolidated_instance_pollster_dict = {
    "counter_name": "instance-m1.small",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "70979ae8-abc8-42dd-856e-19016911f615",
    "display_name": "testcustomer-lamed-70979ae8-abc8-42dd-856e-19016911f615",
    "counter_type": "gauge",
    "counter_unit": "instance",
    "_consolidated": "1",
    "_event": "0"
}

expected_consolidated_instance_pollster_payload = siphash.SipHash24("\0"*16, "2").hash()

instance_event_json = """
{
    "id": "3d83095a-486c-11e4-b39f-525400f01ec9",
    "name": "instance",
    "project_id": "8ee04b8cfdd94a3cafb16566515af9d5",
    "resource_id": "c5b9cb08-e8be-427e-82b2-56ba6da2da42",
    "resource_metadata": {
        "access_ip_v4": null,
        "access_ip_v6": null,
        "architecture": null,
        "audit_period_beginning": "2014-09-22 12:00:00",
        "audit_period_ending": "2014-09-22 13:00:00",
        "availability_zone": "syd1",
        "bandwidth": {},
        "created_at": "2014-09-08T06:10:04.000000",
        "deleted_at": "",
        "disk_gb": 1,
        "display_name": "Tutorial01",
        "ephemeral_gb": 0,
        "event_type": "compute.instance.exists",
        "host": "conductor.a001.api.syd1.prod.os.anchor.net.au",
        "hostname": "tutorial01",
        "image_meta": {
            "base_image_ref": "",
            "description": "Built with diskimage-builder",
            "min_disk": "1"
        },
        "image_ref_url": "http://127.0.0.1:9292/images/",
        "instance_flavor_id": "1",
        "instance_id": "c5b9cb08-e8be-427e-82b2-56ba6da2da42",
        "instance_type": "m1.tiny",
        "instance_type_id": 6,
        "kernel_id": "",
        "launched_at": "2014-09-15T02:08:11.000000",
        "memory_mb": 512,
        "node": "a001.comp.syd1.prod.os.anchor.net.au",
        "os_type": null,
        "ramdisk_id": "",
        "reservation_id": "r-4gtpue0h",
        "root_gb": 1,
        "state": "active",
        "state_description": "",
        "tenant_id": "8ee04b8cfdd94a3cafb16566515af9d5",
        "terminated_at": "",
        "user_id": "189b16e386344dc4a96ba08f8ff97b66",
        "vcpus": 1
    },
    "source": "openstack",
    "timestamp": "2014-09-22 13:00:39.642727",
    "type": "gauge",
    "unit": "instance",
    "user_id": "189b16e386344dc4a96ba08f8ff97b66",
    "volume": 1
}
"""

expected_consolidated_instance_event_dict = {
    "counter_name": "instance",
    "project_id": "8ee04b8cfdd94a3cafb16566515af9d5",
    "resource_id": "c5b9cb08-e8be-427e-82b2-56ba6da2da42",
    "display_name": "Tutorial01",
    "counter_type": "gauge",
    "counter_unit": "instance",
    "_consolidated": "1",
    "_event": "1"
}

expected_consolidated_instance_event_payload = (5 << 8) + (6 << 32)

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
                        "instance_type_id": 14,
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
                    "volume": 1,
                    "timestamp": "1970-01-01 00:00:00",
                    "resource_metadata": {
                    "instance_type": "13",
                        "foo": "bar",
                        "display_name": "bob"
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

def test_remove_extraneous():
    expected = """
                {
                    "name": "disk.read.requests",
                    "project_id": "78e2fc2a70314713b9f814ec634b5e10",
                    "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d",
                    "type": "cumulative",
                    "unit": "request",
                    "user_id": "8288e6719e8c4b6a8b130ae77af7abbc"
                }
"""
    local_json = copy.copy(parsed_json)
    ceilometer_publisher_vaultaire.process.remove_extraneous(local_json)
    expected_json = json.loads(expected)
    assert local_json == expected_json


def test_process_raw():
    process_raw = ceilometer_publisher_vaultaire.process.process_raw

    expectedMiniSd = {"project_id": "123", "resource_id": "456", "counter_name": "instance", "counter_type": "gauge", "_unit": "instance", "display_name": "bob"}
    expectedEventSd = {"project_id": "123", "resource_id": "456", "counter_name": "instance", "counter_type": "gauge", "_unit": "instance", "display_name": "bob", "event_type": "instance.create.end"}
    (addr1, sd1, ts1, p1) = process_raw(copy.copy(parsed_mini_json))
    (addr2, sd2, ts2, p2) = process_raw(copy.copy(parsed_event_json))
    assert sd1 == expectedMiniSd
    assert sd2 == expectedEventSd
    assert sd1 != sd2
    assert ts1 == 0
    assert ts1 == ts2
    assert p1 == 1
    assert p2 == 23
    assert addr1 != addr2

def test_consolidate_instance_flavor():

    parsed_mini_json = json.loads(mini_json)
    (_, sd, ts, p) = ceilometer_publisher_vaultaire.consolidate_instance_flavor(parsed_mini_json)
    expected_sd = {"project_id": "123", "resource_id": "456", "counter_name": "instance", "counter_type": "gauge", "counter_unit": "instance", "display_name": "bob", "_consolidated": "1", "_event": "0"}
    assert sd == expected_sd
    assert p == siphash.SipHash24("\0"*16, "13").hash()
    assert ts == 0
    parsed_instance_pollster_json = json.loads(instance_pollster_json)

    (_, sd, _, p) = ceilometer_publisher_vaultaire.consolidate_instance_flavor(parsed_instance_pollster_json)
    assert sd == expected_consolidated_instance_pollster_dict
    assert p == expected_consolidated_instance_pollster_payload

def test_process_consolidated_event():

    parsed_event_json = json.loads(event_json)
    (_, sd, ts, p) = ceilometer_publisher_vaultaire.process_consolidated_event(parsed_event_json)
    expected_sd =  {"project_id": "123", "resource_id": "456", "counter_name": "instance", "counter_type": "gauge", "counter_unit": "instance", "_consolidated": "1", "_event": "1", "display_name": "bob"}
    assert sd == expected_sd
    assert p == (1 << 8) + (2 << 16) + (14 << 32)
    assert ts == 0
    parsed_instance_event_json = json.loads(instance_event_json)

    (_, sd, _, p) = ceilometer_publisher_vaultaire.process_consolidated_event(parsed_instance_event_json)
    assert sd == expected_consolidated_instance_event_dict
    assert p == expected_consolidated_instance_event_payload

def test_sanitize():
    sanitize = ceilometer_publisher_vaultaire.process.sanitize
    assert sanitize(None)  == ""
    assert sanitize(True)  == 1
    assert sanitize(False) == 0
    assert sanitize("maryhadalittlelamb") == "maryhadalittlelamb"
    assert sanitize("\"numberOf****sGiven\":\"0\"") == "\"numberOf****sGiven\"-\"0\""
    assert sanitize("[this,is,a,list]") == "[this-is-a-list]"

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
    test_remove_extraneous()
    test_process_raw()
    test_consolidate_instance_flavor()
    test_process_consolidated_event()
    test_sanitize()
    test_sanitize_timestamp()

    sys.exit(0)
