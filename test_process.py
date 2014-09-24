import sys

sys.path.insert(0,'./ceilometer_publisher_vaultaire')
from process import process_sample
import json
import pprint

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)



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

sample = DictableDict(json.loads(sample_json))
for (a, b, c, d) in process_sample(sample):
    print ("Timestamp = {}".format(c))
    print ("Payload   = {}".format(d))
    print ("Address   = {}".format(base62_encode(a)))
    print ("SD =")
    pprint.pprint (b)
    print
