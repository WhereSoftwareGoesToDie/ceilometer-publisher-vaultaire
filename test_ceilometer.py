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
volume_json = """
{
    "id": "3137baa6-486c-11e4-b39f-525400f01ec9",
    "name": "volume.size",
    "project_id": "aaaf752c50804cf3aad71b92e6ced65e",
    "resource_id": "6b116a55-2716-4406-9304-0080e3a5c608",
    "resource_metadata": {
        "availability_zone": "syd1",
        "created_at": "2014-09-22 07:31:41",
        "display_name": "Worpress0Snapshot3",
        "event_type": "volume.create.start",
        "host": "volume.a002.cinder.syd1.prod.os.anchor.net.au@block",
        "launched_at": "",
        "size": 10,
        "snapshot_id": "3e4cb913-6348-4c47-a8de-7af3a75099c0",
        "status": "creating",
        "tenant_id": "aaaf752c50804cf3aad71b92e6ced65e",
        "user_id": "d23fdb56933e41b58d2edebb57c1e8a6",
        "volume_id": "6b116a55-2716-4406-9304-0080e3a5c608",
        "volume_type": "7a522201-7c27-4eaa-9d95-d70cfaaeb16a"
    },
    "source": "openstack",
    "timestamp": "2014-09-22 07:31:41.378773",
    "type": "gauge",
    "unit": "GB",
    "user_id": "d23fdb56933e41b58d2edebb57c1e8a6",
    "volume": 10
}
"""

expected_volume_payload = 2 + (1 << 8) + (1 << 16) + (10 << 32)
expected_volume_timestamp = 1411371101378773000
expected_volume_sd = {
    "_event": "1",
    "_consolidated": "1",
    "project_id": "aaaf752c50804cf3aad71b92e6ced65e",
    "resource_id": "6b116a55-2716-4406-9304-0080e3a5c608",
    "metric_name": "volume.size",
    "metric_unit": "GB",
    "metric_type": "gauge",
    "display_name": "Worpress0Snapshot3"
}

ip_json = """
{
    "id": "31a00552-486c-11e4-b39f-525400f01ec9",
    "name": "ip.floating",
    "project_id": "aaaf752c50804cf3aad71b92e6ced65e",
    "resource_id": "d1d96a82-c1ce-4feb-acc1-227a50be9b9f",
    "resource_metadata": {
        "event_type": "floatingip.create.end",
        "fixed_ip_address": null,
        "floating_ip_address": "110.173.152.166",
        "floating_network_id": "3e7c812b-7e44-4255-9db0-8bfac34cf4b7",
        "host": "network.a001.api.syd1.prod.os.anchor.net.au",
        "id": "d1d96a82-c1ce-4feb-acc1-227a50be9b9f",
        "port_id": null,
        "router_id": null,
        "status": "DOWN",
        "tenant_id": "aaaf752c50804cf3aad71b92e6ced65e"
    },
    "source": "openstack",
    "timestamp": "2014-09-22 07:35:03.030569",
    "type": "gauge",
    "unit": "ip",
    "user_id": "d23fdb56933e41b58d2edebb57c1e8a6",
    "volume": 1
}
"""

expected_ip_payload = 2 + (1 << 8) + (2 << 16) + (1 << 32)
expected_ip_timestamp = 1411371303030569000
expected_ip_sd = {
    "_event": "1",
    "_consolidated": "1",
    "project_id": "aaaf752c50804cf3aad71b92e6ced65e",
    "resource_id": "d1d96a82-c1ce-4feb-acc1-227a50be9b9f",
    "metric_name": "ip.floating",
    "metric_unit": "ip",
    "metric_type": "gauge",
    "display_name": ""
}

instance_pollster_json = """
{
    "id": "90b350ac-4a07-11e4-a84e-ecf4bbc1fce4",
    "name": "instance",
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

expected_instance_timestamp = 1412235708000000000

expected_instance_flavor_payload = siphash.SipHash24("\0"*16, "2").hash()
expected_instance_flavor_sd = {
    "metric_name": "instance_flavor",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "70979ae8-abc8-42dd-856e-19016911f615",
    "display_name": "testcustomer-lamed-70979ae8-abc8-42dd-856e-19016911f615",
    "metric_type": "gauge",
    "metric_unit": "instance",
    "_consolidated": "1",
    "_event": "0"
}
expected_instance_ram_payload = 2048
expected_instance_ram_sd = {
    "metric_name": "instance_ram",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "70979ae8-abc8-42dd-856e-19016911f615",
    "display_name": "testcustomer-lamed-70979ae8-abc8-42dd-856e-19016911f615",
    "metric_type": "gauge",
    "metric_unit": "MB",
    "_consolidated": "1",
    "_event": "0"
}
expected_instance_vcpus_payload = 1
expected_instance_vcpus_sd = {
    "metric_name": "instance_vcpus",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "70979ae8-abc8-42dd-856e-19016911f615",
    "display_name": "testcustomer-lamed-70979ae8-abc8-42dd-856e-19016911f615",
    "metric_type": "gauge",
    "metric_unit": "vcpu",
    "_consolidated": "1",
    "_event": "0"
}
expected_instance_disk_payload = 20
expected_instance_disk_sd = {
    "metric_name": "instance_disk",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "70979ae8-abc8-42dd-856e-19016911f615",
    "display_name": "testcustomer-lamed-70979ae8-abc8-42dd-856e-19016911f615",
    "metric_type": "gauge",
    "metric_unit": "GB",
    "_consolidated": "1",
    "_event": "0"
}

network_rx_json = """
{
    "id": "ba37392a-4a93-11e4-b541-ecf4bbc1fce4",
    "name": "network.incoming.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "instance-000009fd-d9067afa-c79c-4a37-8181-84f612da2d48-tapad6aebca-fe",
    "resource_metadata": {
        "fref": null,
        "instance_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
        "instance_type": "2",
        "mac": "fa:16:3e:ca:15:d7",
        "name": "tapad6aebca-fe",
        "parameters": {}
    },
    "source": "openstack",
    "timestamp": "2014-10-03T00:25:07Z",
    "type": "cumulative",
    "unit": "B",
    "user_id": "1f6f57489d404fe293c65d19e9b31df2",
    "volume": 58832
}
"""
expected_network_rx_payload = 58832
expected_network_rx_timestamp = 1412295907000000000
expected_network_rx_sd = {
    "metric_name": "network.incoming.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "instance-000009fd-d9067afa-c79c-4a37-8181-84f612da2d48-tapad6aebca-fe",
    "display_name": "",
    "metric_type": "cumulative",
    "metric_unit": "B",
    "_consolidated": "1",
    "_event": "0",
    "_counter": "1"
}

network_tx_json = """
{
    "id": "c927a410-4a93-11e4-b541-ecf4bbc1fce4",
    "name": "network.outgoing.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "instance-000009fd-d9067afa-c79c-4a37-8181-84f612da2d48-tapad6aebca-fe",
    "resource_metadata": {
        "fref": null,
        "instance_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
        "instance_type": "2",
        "mac": "fa:16:3e:ca:15:d7",
        "name": "tapad6aebca-fe",
        "parameters": {}
    },
    "source": "openstack",
    "timestamp": "2014-10-03T00:25:32Z",
    "type": "cumulative",
    "unit": "B",
    "user_id": "1f6f57489d404fe293c65d19e9b31df2",
    "volume": 21816
}
"""
expected_network_tx_payload = 21816
expected_network_tx_timestamp = 1412295932000000000
expected_network_tx_sd = {
    "metric_name": "network.outgoing.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "instance-000009fd-d9067afa-c79c-4a37-8181-84f612da2d48-tapad6aebca-fe",
    "display_name": "",
    "metric_type": "cumulative",
    "metric_unit": "B",
    "_consolidated": "1",
    "_event": "0",
    "_counter": "1"
}

disk_write_json = """
{
    "id": "d912463c-4a93-11e4-b541-ecf4bbc1fce4",
    "name": "disk.write.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
    "resource_metadata": {
        "OS-EXT-AZ:availability_zone": "syd1",
        "disk_gb": 20,
        "display_name": "testcustomer-pe-d9067afa-c79c-4a37-8181-84f612da2d48",
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
        "name": "instance-000009fd",
        "ramdisk_id": null,
        "root_gb": 20,
        "vcpus": 1
    },
    "source": "openstack",
    "timestamp": "2014-10-03T00:25:59Z",
    "type": "cumulative",
    "unit": "B",
    "user_id": "1f6f57489d404fe293c65d19e9b31df2",
    "volume": 12387328
}
"""
expected_disk_write_payload = 12387328
expected_disk_write_timestamp = 1412295959000000000
expected_disk_write_sd = {
    "metric_name": "disk.write.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
    "display_name": "testcustomer-pe-d9067afa-c79c-4a37-8181-84f612da2d48",
    "metric_type": "cumulative",
    "metric_unit": "B",
    "_consolidated": "1",
    "_event": "0",
    "_counter": "1"
}

disk_read_json = """
{
    "id": "d9e137b2-4a93-11e4-b541-ecf4bbc1fce4",
    "name": "disk.read.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
    "resource_metadata": {
        "OS-EXT-AZ:availability_zone": "syd1",
        "disk_gb": 20,
        "display_name": "testcustomer-pe-d9067afa-c79c-4a37-8181-84f612da2d48",
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
        "name": "instance-000009fd",
        "ramdisk_id": null,
        "root_gb": 20,
        "vcpus": 1
    },
    "source": "openstack",
    "timestamp": "2014-10-03T00:26:00Z",
    "type": "cumulative",
    "unit": "B",
    "user_id": "1f6f57489d404fe293c65d19e9b31df2",
    "volume": 117644800
}
"""

expected_disk_read_payload = 117644800
expected_disk_read_timestamp = 1412295960000000000
expected_disk_read_sd = {
    "metric_name": "disk.read.bytes",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
    "display_name": "testcustomer-pe-d9067afa-c79c-4a37-8181-84f612da2d48",
    "metric_type": "cumulative",
    "metric_unit": "B",
    "_consolidated": "1",
    "_event": "0",
    "_counter": "1"
}

cpu_json = """
{
    "id": "dac5d476-4a93-11e4-b541-ecf4bbc1fce4",
    "name": "cpu",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
    "resource_metadata": {
        "OS-EXT-AZ:availability_zone": "syd1",
        "cpu_number": 1,
        "disk_gb": 20,
        "display_name": "testcustomer-pe-d9067afa-c79c-4a37-8181-84f612da2d48",
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
        "name": "instance-000009fd",
        "ramdisk_id": null,
        "root_gb": 20,
        "vcpus": 1
    },
    "source": "openstack",
    "timestamp": "2014-10-03T00:26:01Z",
    "type": "cumulative",
    "unit": "ns",
    "user_id": "1f6f57489d404fe293c65d19e9b31df2",
    "volume": 49320000000
}
"""

expected_cpu_payload = 49320000000
expected_cpu_timestamp = 1412295961000000000
expected_cpu_sd = {
    "metric_name": "cpu",
    "project_id": "da1ea3cce8b545f382e0e1ca8f863c22",
    "resource_id": "d9067afa-c79c-4a37-8181-84f612da2d48",
    "display_name": "testcustomer-pe-d9067afa-c79c-4a37-8181-84f612da2d48",
    "metric_type": "cumulative",
    "metric_unit": "ns",
    "_consolidated": "1",
    "_event": "0",
    "_counter": "1"
}

disk_write_requests_json = """
{
    "id": "e83fc9ea-4a93-11e4-b541-ecf4bbc1fce4",
    "name": "disk.write.requests",
    "project_id": "1dcad6d961334ccc8a3d55ba017ffe80",
    "resource_id": "093517a5-f3bd-4389-ad50-6cf86b3e6396",
    "resource_metadata": {
        "OS-EXT-AZ:availability_zone": "syd1",
        "disk_gb": 1,
        "display_name": "sio-resh-093517a5-f3bd-4389-ad50-6cf86b3e6396",
        "ephemeral_gb": 0,
        "flavor": {
            "disk": 1,
            "ephemeral": 0,
            "id": "1",
            "links": [
                {
                    "href": "http://nova.int.syd1.prod.os.anchor.net.au:8774/bd3f518204b54407854a7dd8d1c49931/flavors/1",
                    "rel": "bookmark"
                }
            ],
            "name": "m1.tiny",
            "ram": 512,
            "vcpus": 1
        },
        "host": "f3c773da31821e60fcb75af58952096da520679f176605c21033feac",
        "image": null,
        "image_ref": null,
        "image_ref_url": null,
        "instance_type": "1",
        "kernel_id": null,
        "memory_mb": 512,
        "name": "instance-000008ff",
        "ramdisk_id": null,
        "root_gb": 1,
        "vcpus": 1
    },
    "source": "openstack",
    "timestamp": "2014-10-03T00:26:24Z",
    "type": "cumulative",
    "unit": "request",
    "user_id": "1f6f57489d404fe293c65d19e9b31df2",
    "volume": 3624
}
"""
instance_tiny_json = """
{
    "id": "e943094c-4a93-11e4-b541-ecf4bbc1fce4",
    "name": "instance:m1.tiny",
    "project_id": "5e4b3c183e3f4c4aa5e5e8b78a6c80c2",
    "resource_id": "d94bc995-a2ab-4a9a-9405-fbec69828bc7",
    "resource_metadata": {
        "OS-EXT-AZ:availability_zone": "syd1",
        "disk_gb": 1,
        "display_name": "louie.entropyonwheels.com",
        "ephemeral_gb": 0,
        "flavor": {
            "disk": 1,
            "ephemeral": 0,
            "id": "1",
            "links": [
                {
                    "href": "http://nova.int.syd1.prod.os.anchor.net.au:8774/bd3f518204b54407854a7dd8d1c49931/flavors/1",
                    "rel": "bookmark"
                }
            ],
            "name": "m1.tiny",
            "ram": 512,
            "vcpus": 1
        },
        "host": "25e409871a7dc4f271bb08de1c4f6beaf87c53f9ffef7cef5693bc5a",
        "image": null,
        "image_ref": null,
        "image_ref_url": null,
        "instance_type": "1",
        "kernel_id": null,
        "memory_mb": 512,
        "name": "instance-000002cb",
        "ramdisk_id": null,
        "root_gb": 1,
        "vcpus": 1
    },
    "source": "openstack",
    "timestamp": "2014-10-03T00:26:26Z",
    "type": "gauge",
    "unit": "instance",
    "user_id": "29ce915f6f80426f8d059e6495e4441a",
    "volume": 1
}
"""

def test_process_sample():
    for js in [network_rx_json, network_tx_json, cpu_json, disk_write_json, disk_read_json, ip_json, volume_json]:
        parsed_json = json.loads(js)
        result_list = ceilometer_publisher_vaultaire.process_sample(parsed_json)
        assert len(result_list) == 1
    for js in [disk_write_requests_json, instance_tiny_json]:
        parsed_json = json.loads(js)
        result_list = ceilometer_publisher_vaultaire.process_sample(parsed_json)
        assert len(result_list) == 0

def test_consolidate_volume_event():
    parsed_volume_json = json.loads(volume_json)
    result = ceilometer_publisher_vaultaire.consolidate_volume_event(parsed_volume_json)
    compare_result(result, expected_volume_sd, expected_volume_timestamp, expected_volume_payload)

def test_consolidate_ip_event():
    parsed_ip_json = json.loads(ip_json)
    result = ceilometer_publisher_vaultaire.consolidate_ip_event(parsed_ip_json)
    compare_result(result, expected_ip_sd, expected_ip_timestamp, expected_ip_payload)

def test_consolidate_instance():
    parsed_instance_pollster_json = json.loads(instance_pollster_json)
    result_list = ceilometer_publisher_vaultaire.process_sample(parsed_instance_pollster_json)
    assert len(result_list) == 4
    flavor_result = result_list[0]
    ram_result = result_list[1]
    vcpus_result = result_list[2]
    disk_result = result_list[3]

    compare_result(flavor_result, expected_instance_flavor_sd, expected_instance_timestamp, expected_instance_flavor_payload)
    compare_result(ram_result, expected_instance_ram_sd, expected_instance_timestamp, expected_instance_ram_payload)
    compare_result(vcpus_result, expected_instance_vcpus_sd, expected_instance_timestamp, expected_instance_vcpus_payload)
    compare_result(disk_result, expected_instance_disk_sd, expected_instance_timestamp, expected_instance_disk_payload)

def test_process_base_pollster():
    result = ceilometer_publisher_vaultaire.process_base_pollster(json.loads(network_rx_json))
    compare_result(result, expected_network_rx_sd, expected_network_rx_timestamp, expected_network_rx_payload)

    result = ceilometer_publisher_vaultaire.process_base_pollster(json.loads(network_tx_json))
    compare_result(result, expected_network_tx_sd, expected_network_tx_timestamp, expected_network_tx_payload)

    result = ceilometer_publisher_vaultaire.process_base_pollster(json.loads(disk_write_json))
    compare_result(result, expected_disk_write_sd, expected_disk_write_timestamp, expected_disk_write_payload)

    result = ceilometer_publisher_vaultaire.process_base_pollster(json.loads(disk_read_json))
    compare_result(result, expected_disk_read_sd, expected_disk_read_timestamp, expected_disk_read_payload)

    result = ceilometer_publisher_vaultaire.process_base_pollster(json.loads(cpu_json))
    compare_result(result, expected_cpu_sd, expected_cpu_timestamp, expected_cpu_payload)

def compare_result(result, expected_sd, expected_ts, expected_p):
    (_, sd, ts, p) = result
    assert sd == expected_sd
    assert ts == expected_ts
    assert p  == expected_p

def test_sanitize():
    sanitize = ceilometer_publisher_vaultaire.process.sanitize
    assert sanitize(None)  == ""
    assert sanitize(True)  == 1
    assert sanitize(False) == 0
    assert sanitize("maryhadalittlelamb") == "maryhadalittlelamb"
    assert sanitize("\"numberOf****sGiven\":\"0\"") == "\"numberOf****sGiven\"-\"0\""
    assert sanitize("[this,is,a,list]") == "[this-is-a-list]"

def test_sanitize_timestamp():
    sanitize_timestamp = ceilometer_publisher_vaultaire.process.sanitize_timestamp
    assert sanitize_timestamp("1970-01-01 00:00:00") == 0
    assert sanitize_timestamp("1970-01-01T00:00:00Z") == 0
    assert sanitize_timestamp("1970-01-01 00:00:00-0200") == 7200*10**9
    assert sanitize_timestamp("1993-03-17T21:00:00+1000") == 732366000*10**9

if __name__ == '__main__':
    test_consolidate_instance()
    test_consolidate_volume_event()
    test_consolidate_ip_event()
    test_process_base_pollster()
    test_process_sample()
    test_sanitize()
    test_sanitize_timestamp()

    sys.exit(0)
