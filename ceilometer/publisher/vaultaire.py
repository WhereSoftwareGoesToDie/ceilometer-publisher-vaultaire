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


# Parse a textual timestamp into a timezone-aware timestamp.
def parse_date_string(date_string):
    timestamp = parse(date_string)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=tzutc())
    return timestamp

# Take a nested dictionary, and flatten it into a one-level dictionary
# Also parses and removes any keyvalues that Marquise/Vaultaire can't handle.
def flatten(n, prefix=""):
    flattened_dict = {}
    for k,v in n.items():
        # Remove Colons from Keys
        k = k.replace(':', '~')
        
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
            if type(v) is str:
                    # Remove colons from Values
		    v = v.replace(":","-")
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
		if "audit_period_beginning" in sample:
			print sample["resource_metadata"]["audit_period_beginning"]	
	
		if "launched_at" in sample:
			print sample["resource_metadata"]["launched_at"]


		# Generate the unique identifer for the sample	
                identifier = (sample["resource_id"] + sample["project_id"] + sample["name"] + sample["type"] + sample["unit"])
                address = Marquise.hash_identifier(identifier)

		# Parse timestamp to nanoseconds since epoch
                timestamp = int(parse_date_string(sample["timestamp"]).strftime("%s") +"000000000")

		# Vaultaire cares about the datatype of the payload
                payload = sample["volume"]
		ptype = type(payload)
                
		_float = None
                _extended = None

                if ptype == float:
                    _float = 1
                elif ptype == str:
                    _extended = 1
                elif ptype == bool:
                    payload = 1 if sample["volume"] == True else 0
               
		# Rebuild the sample as a source dict 
		sourcedict = dict(sample)

		# Cast unit as a special metadata type
                sourcedict["_unit"] = sample.pop("unit")
                
                # Remove elements that we know to always change (not very useful for a source dictionary)
                del sourcedict["timestamp"]
                del sourcedict["volume"]

		# Remove the original resource_metadata and substitute our own flattened version
                del sourcedict["resource_metadata"]
                sourcedict.update(flatten(sample["resource_metadata"]))

		# Finally, send it all off to marquise
		LOG.debug(_("Marquise Send Simple: %s %s %s") % (address, timestamp, payload))
                marq.send_simple(address=address, timestamp=timestamp, value=payload)
	
		LOG.debug(_("Marquise Update Source Dict for %s - %s") % (address, pformat(sourcedict)))
                marq.update_source(address, sourcedict)

