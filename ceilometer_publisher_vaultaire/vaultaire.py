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

import payload as p
import process

LOG = log.getLogger(__name__)

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
                processed = process_sample(sample)
                for (address, sourcedict, timestamp, payload) in processed:
                    # Send it all off to marquise
                    LOG.info(_("Marquise Send Simple: %s %s %s") % (address, timestamp, payload))
                    marq.send_simple(address=address, timestamp=timestamp, value=payload)

                    LOG.debug(_("Marquise Update Source Dict for %s - %s") % (address, pformat(sourcedict)))
                    marq.update_source(address, sourcedict)
