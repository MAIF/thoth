package fr.maif.eventsourcing;

public interface AutoSnapshotingStrategy {
    boolean shouldSnapshot(Integer eventCount);


    record NoOpSnapshotingStrategy() implements AutoSnapshotingStrategy {
        @Override
        public boolean shouldSnapshot(Integer eventCount) {
            return false;
        }
    }

    record CountSnapshotingStrategy(int count) implements AutoSnapshotingStrategy {
        @Override
        public boolean shouldSnapshot(Integer eventCount) {
            return eventCount > count;
        }
    }
}
