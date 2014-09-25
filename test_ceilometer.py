"""We can't do much real ceilometer work, but we can test the functional
bits of our code.
"""

import sys
import json
import pprint
from hashlib import sha1
import ceilometer_publisher_vaultaire

class MarquiseFakeParsedUrl(object):
    """Pretend to be a ceilometer publisher URL."""
    def __init__(self, namespace):
        self.netloc = namespace

class DictableDict(object):
    """Wrap a dict in an object that provides an as_dict method."""
    def __init__(self, yourDict):
        self.yourDict = yourDict
    def as_dict(self):
        """Totally return a dict of that dict."""
        return self.yourDict

sample_json = """
                {
                    "id": "a994b3b8-21c6-11e4-8d23-a60a09774fec",
                    "name": "disk.read.requests",
                    "project_id": "78e2fc2a70314713b9f814ec634b5e10",
                    "resource_id": "cda0c65e-0fc6-4810-89d6-b5c78745f12d",
                    "resource_metadata": {
                        "OS-EXT-AZ:availability_zone": "nova",
                        "disk_gb": 0,
                        "display_name": "bar2",
                        "ephemeral_gb": 0,
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
                        "image": {
                            "id": "3c6d80d7-8253-42ee-8f60-fb837845c161",
                            "links": [
                                {
                                    "href": "http://192.168.42.114:8774/0632438bb4a748218b43ed9223b39f77/images/3c6d80d7-8253-42ee-8f60-fb837845c161",
                                    "rel": "bookmark"
                                }
                            ],
                            "name": "Fedora-x86_64-20-20140618-sda"
                        },
                        "image_ref": "3c6d80d7-8253-42ee-8f60-fb837845c161",
                        "image_ref_url": "http://192.168.42.114:8774/0632438bb4a748218b43ed9223b39f77/images/3c6d80d7-8253-42ee-8f60-fb837845c161",
                        "instance_type": "42",
                        "kernel_id": null,
                        "memory_mb": 64,
                        "name": "instance-00000002",
                        "ramdisk_id": null,
                        "root_gb": 0,
                        "status": "active",
                        "vcpus": 1
                    },
                    "source": "openstack",
                    "timestamp": "2014-08-12T02:16:26Z",
                    "type": "cumulative",
                    "unit": "request",
                    "user_id": "8288e6719e8c4b6a8b130ae77af7abbc",
                    "volume": 332
                }


"""
#parsed_json = DictableDict(json.loads(sample_json))
#samples = [parsed_json]
samples = [ DictableDict(json.loads(x)) for x in [sample_json] ]
normalised_sample_json = json.dumps(json.loads(sample_json))
# XXX: Fuck inconsistent ordering of dicts, fix this tomorrow
print("OrigSample JSON hashes to:    {}".format(sha1(sample_json.encode('utf8')).hexdigest()))
print("NormSample JSON hashes to:    {}".format(sha1(normalised_sample_json.encode('utf8')).hexdigest()))

flattened_json = ceilometer_publisher_vaultaire.flatten(json.loads(sample_json))
print("Flattened JSON hashes to: {}".format(sha1(json.dumps(flattened_json).encode('utf8')).hexdigest()))


url = MarquiseFakeParsedUrl('testnamespace')

#v = ceilometer_publisher_vaultaire.VaultairePublisher(url)
#v.publish_samples(None, samples)
