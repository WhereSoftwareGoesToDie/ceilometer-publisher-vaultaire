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
"""


from ceilometer import publisher

from marquise import Marquise

from pprint import pformat
import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc

from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log

LOG = log.getLogger(__name__)


def sanitize(v):
    """Sanitize a value into something that Marquise can use. This weeds
    out None keys/values, and ensures that timestamps are consistently
    formatted.
    """
    if v is None:
        return ""
    if v in (True,False):
        return 1 if v is True else 0
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


# pylint: disable=too-few-public-methods
class VaultairePublisher(publisher.PublisherBase):
    """Implements the Publisher interface for Ceilometer."""
    def __init__(self, parsed_url):
        super(VaultairePublisher, self).__init__(parsed_url)

        self.marquise = None
        namespace = parsed_url.netloc
        if not namespace:
            LOG.error(_('The namespace for the vaultaire publisher is required'))
            return

        LOG.info(_("Marquise loaded with namespace %s" % namespace))
        self.marquise = Marquise(namespace)

    def publish_samples(self, dummy_context, samples):
        """Reconstruct a metering message for publishing to Vaultaire via Marquise

        :param dummy_context: Execution context from the service or RPC call. (Unused)
        :param samples: Samples from pipeline after transformation
        """
        if self.marquise:
            marq = self.marquise
            for sample in samples:
                sample = sample.as_dict()
                LOG.debug(_("Vaultaire Publisher got sample:\n%s") % pformat(sample))

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
                sourcedict["_unit"] = sourcedict.pop("unit")

                # If it's a cumulative value, we need to tell vaultaire
                if sourcedict["type"] == "cumulative":
                    sourcedict["_counter"] = 1

                # Cast Identifier sections with unique names, in case of metadata overlap
                sourcedict["counter_name"] = sourcedict.pop("name")
                sourcedict["counter_type"] = sourcedict.pop("type")

                # Remove elements that we know to always change (not very useful for a source dictionary)
                del sourcedict["timestamp"]
                del sourcedict["volume"]

                # Remove the original resource_metadata and substitute our own flattened version
                sourcedict.update(flatten(sourcedict.pop("resource_metadata")))

                # Finally, send it all off to marquise
                LOG.info(_("Marquise Send Simple: %s %s %s") % (address, timestamp, payload))
                marq.send_simple(address=address, timestamp=timestamp, value=payload)

                LOG.debug(_("Marquise Update Source Dict for %s - %s") % (address, pformat(sourcedict)))
                marq.update_source(address, sourcedict)
