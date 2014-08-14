#
# Authors: Barney Desmond   <barneydesmond@gmail.com>
#          Katie McLaughlin <katie@glasnt.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Publish a sample (metric data point) to Vaultaire

To install:
    ceilometer.egg-info/entry_points.txt
    > [ceilometer.publishers]
    > vaultaire = ceilometer.publisher.vaultaire:VaultairePublisher

    git clone https://github.com/anchor/libmarquise.git
    git clone https://github.com/anchor/pymarquise.git

    cd libmarquise; and follow README install docs
    cd pymarquise; and `python setup.py install`

    if ceilometer user can't access /var/spool:
      mkdir /var/spool/marquise,
      chown user:user /var/spool/marquise

    THEN

    Add vaultaire to part of your pipeline.yaml sink, like:
       publishers:
           - vaultaire://namespace

"""


from ceilometer import publisher

from marquise import Marquise
import datetime

from pprint import pprint
from pprint import pformat
from dateutil.parser import *
from dateutil.tz import *

from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log

LOG = log.getLogger(__name__)


# Sanitize a value into something that Marquise can use
def sanitize(v):
    try:
        # Try and take a value and use dateutil to parse it
        timestamp = parse(v)
        if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=tzutc())
        return int(timestamp.strftime("%s") +"000000000")
    except:
        # Should dateutil parsing fail, attempt sanitation if it's a primative type
        if type(v) is str:
            v = v.replace(":","-")
            v = v.replace(",","-")
        elif type(v) is bool:
            v = 1 if v == True else 0
        return v

# Take a nested dictionary, and flatten it into a one-level dictionary
# Also removes any keyvalues that Marquise/Vaultaire can't handle.
def flatten(n, prefix=""):
    flattened_dict = {}
    for k,v in n.items():
        k = sanitize(k)

        # Vaultaire doesn't care about generated URLs for ceilometer API references.
        if k == "links":
            continue
        if k.endswith('_url'):
            continue

        # Vaultaire doesn't want values if they have no contents
        if v is None:
            continue

        # If key has a parent, concatenate it into the new keyname
        if prefix != "":
            k = "{}~{}".format(prefix,k)

        # This was previously a check for __iter__, but strings have those now,
        # so let's just check for dict-ness instead. No good on lists anyway.
        if type(v) is not dict:
            v = sanitize(v)
            flattened_dict[k] = v
        else:
            flattened_dict.update(flatten(v, k))
    return flattened_dict


class VaultairePublisher(publisher.PublisherBase):
    def __init__(self, parsed_url):
        super(VaultairePublisher, self).__init__(parsed_url)

        self.marquise = None
        namespace = parsed_url.netloc
        if not namespace:
            LOG.error(_('The namespace for the vaultaire publisher is required'))
            return

        LOG.info(_("Marquise loaded with namespace %s" % namespace))
        self.marquise = Marquise(namespace)

    def publish_samples(self, context, samples):
        """Reconstruct a metering message for publishing to Vaultaire via Marquise

        :param context: Execution context from the service or RPC call
        :param samples: Samples from pipeline after transformation
        """
        if self.marquise:
            marq = self.marquise
            for sample in samples:
                sample = sample.as_dict()
                LOG.info(_("Vaultaire Publisher got sample:\n%s") % pformat(sample))

                # Generate the unique identifer for the sample
                identifier = sample["resource_id"] + sample["project_id"] + \
                             sample["name"] + sample["type"] + sample["unit"]
                address = Marquise.hash_identifier(identifier)

                # Sanitize timestamp (will parse timestamp to nanoseconds since epoch)
                timestamp = sanitize(sample["timestamp"])

                # Our payload is the volume (later parsed to "counter_volume" in ceilometer)
                payload = sanitize(sample["volume"])

                # Rebuild the sample as a source dict 
                sourcedict = dict(sample)

                # Vaultaire cares about the datatype of the payload
                if type(payload) == float:
                    sourcedict["_float"] = 1
                elif type(payload) == str:
                    sourcedict["_extended"] = 1

                # Cast unit as a special metadata type
                sourcedict["_unit"] = sample.pop("unit")

                # Remove elements that we know to always change (not very useful for a source dictionary)
                del sourcedict["timestamp"]
                del sourcedict["volume"]

                # Remove the original resource_metadata and substitute our own flattened version
                sourcedict.update(flatten(sourcedict.pop("resource_metadata")))

                # Finally, send it all off to marquise
                LOG.debug(_("Marquise Send Simple: %s %s %s") % (address, timestamp, payload))
                marq.send_simple(address=address, timestamp=timestamp, value=payload)

                LOG.debug(_("Marquise Update Source Dict for %s - %s") % (address, pformat(sourcedict)))
                marq.update_source(address, sourcedict)

