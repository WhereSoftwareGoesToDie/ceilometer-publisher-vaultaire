#
# Authors: Barney Desmond   <barneydesmond@gmail.com>
#          Katie McLaughlin <katie@glasnt.com>
#          Oswyn Brent      <oswyn.brent@anchor.com.au>
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

RAW_PAYLOAD_IP_ALLOC = 1

def constructPayload(event_type, message, rawPayload):
    """
    eventResolution is passed in message, if none is given assumed to be Success
    eventResolution takes up the LSByte
    eventVerb is the action of the event, e.g. create, delete, shutdown, etc.
    eventVerb takes up the 2nd LSByte
    eventEndpoint defines whether the data represents an event start, end or an
    instance event.
    eventVerb takes up the 3rd LSByte
    rawPayload takes up the 4 MSBytes (32 bits)
    rawPayload is specific on counter
    Start events are even, end are odd (LSB)
    Other 7 bits for create, update, end, and any future additions
    """

    # If no event endpoint is included, assume instantaneous event (e.g. image deletion)
    eventEndpoint = 0

    if message == "Success":
        eventResolution = 0
    # Empty message implies success
    elif message == "":
        eventResolution = 0
    elif message == "Failure":
        eventResolution = 1
    elif message == "error":
        eventResolution = 2

    if event_type.startswith("image"):
        _,verb = event_type.rsplit('.', 1)
    # FIXME: make sure this doesn't break
    else:
        _,verb,endpoint = event_type.rsplit('.', 2)
        eventEndpoint = {"start":1, "end":2}.get(endpoint)

    eventVerb = {"create":1, "update":2, "delete":3, "shutdown":4, "exists":5}.get(verb)

    if eventResolution is None:
        raise Exception("Unsupported message given to eventToByte")
    if eventVerb is None:
        raise Exception("Unsupported event class given to eventToByte")
    if eventEndpoint is None:
        raise Exception("Unsupported event endpoint given to eventToByte")

    return eventResolution + eventVerb << 8 + eventEndpoint << 16 + rawPayload << 32

# FIXME: don't hardcode instance sizes, stick them in a config file
# somewhere, or just use the type ID.
def instanceToRawPayload(instanceType):
    if instanceType == "m1.tiny":
        ret = 1
    elif instanceType == "m1.small":
        ret = 2
    elif instanceType == "m1.medium":
        ret = 3
    elif instanceType == "m1.large":
        ret = 4
    elif instanceType == "m1.xlarge":
        ret = 5
    else:
        ret = 0
        raise Exception("Unsupported instance type given to instanceToPayload")
    return ret

def volumeToRawPayload(volume):
    return volume