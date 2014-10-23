# Well, what do we meter from Openstack?

* CPU Usage          (Nova - Pollster)
* Disk I/O           (Nova - Pollster)
* Neutron Traffic    (Nova - Pollster)
* VCPU Allocations   (Nova - Pollster)
* RAM  Allocation    (Nova - Pollster)
* Disk Allocation    (Nova - Pollster)
* Instance Flavor    (Nova - Pollster)
* IPv4 Allocations   (Neutron - Event)
* Volume Allocations (Cinder  - Event)


# Core Attributes

* `metric_type = sample["type"]`
* `metric_unit = sample["unit"]`
* `payload     = sample["volume"]`
* `timestamp   = sample["timestamp"]`
* `project_id  = sample["project_id"]`
* `resource_id = sample["resource_id"]`

# Event Only Attributes

* `event_type = sample["resource_metadata"]["event_type"]`

# Metric Specifics

## CPU Usage

No Special Requirements

### Match on:

* `sample["name"] = "cpu"`

### Output:

* `metric_name = "cpu"`


## Disk I/O

No Special Requirements

### Match on:

* `sample["name"] = "disk.write.bytes"`
* `sample["name"] = "disk.read.bytes"`

### Output:

* `metric_name = "disk.write.bytes"`
* `metric_name = "disk.read.bytes"`

## Neutron Traffic

No Special Requirements

### Match on:

* `sample["name"] = "network.incoming.bytes"`
* `sample["name"] = "network.outgoing.bytes"`

### Output:

* `metric_name = "network.incoming.bytes"`
* `metric_name = "network.outgoing.bytes"`

## Flavor, Disk, VCPU, Memory Allocations

### Match on:

* `sample["name"] == "instance"`

### Output:

#### Instance Disk Allocation

* `metric_name = "instance_disk"`
* `payload = sample["resource_metadata"]["flavor"]["disk"] + sample["resource_metadata"]["flavor"]["ephemeral"]`

#### VCPU Allocation

* `metric_name = "instance_vcpus"`
* `payload = sample["resource_metadata"]["flavor"]["vcpus"]`

#### Memory Allocation

* ` metric_name = "instance_ram"`
* `payload = sample["resource_metadata"]["flavor"]["ram"]`

#### Flavor

* `metric_name = "instance_flavor"`
* `payload = siphash(sample["resource_metadata"]["instance_type"])`

## IPv4 Allocations

Uses Compound Consolidated Points

### Event Types:

* `floatingip.create.end`
* `floatingip.update.end`

floating.*.start notifications have not been observed but are theoretically possible

### Possible Resource Statuses:

* `ACTIVE`
* `DOWN`

### Match on:

* `sample["name"] = "ip.floating"`

### Output:

* `raw_payload = 1`
* `event_type = sample["resource_metadata"]["event_type"]`
* `split_event_type = event_type.split('.')`
* `verb = split_event_type[1]`
* `endpoint = split_event_type[2]`
* `status = sample["resource_metadata"]["status"]`
* `payload = consolidate_volume(raw_payload, verb, endpoint, status)`

## Volume Allocations

Uses Compound Consolidated Points

### Event Types:

* `volume.create.end`
* `volume.create.start`
* `volume.delete.end`
* `volume.delete.start`
* `volume.resize.end`
* `volume.resize.start`

### Possible Resource Statuses:

* `available`
* `creating`
* `deleting`
* `extending`

### Match On:

* `sample["name"] = volume.size`

### Output:

* `raw_volume = sample["volume"]`
* `event_type = sample["resource_metadata"]["event_type"]`
* `split_event_type = event_type.split('.')`
* `verb = split_event_type[1]`
* `endpoint = split_event_type[2]`
* `status = sample["resource_metadata"]["status"]`
* `payload = consolidate_volume(raw_payload, verb, endpoint, status)`
