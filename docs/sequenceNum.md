# State's sequence num

The `State` interface asks to implement sequenceNum related operations.

This sequence num can be used when storing snapshots of an entity.
These snapshots can be used to speed-up computing of the entity's latest state by reading the snapshot, and applying to it only events with sequenceNum higher than its own.