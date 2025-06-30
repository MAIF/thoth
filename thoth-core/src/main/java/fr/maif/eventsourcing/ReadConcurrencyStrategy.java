package fr.maif.eventsourcing;

/**
 * Journal access strategy
 * <ul>
 *  <li>NO_STRATEGY: nothing is done</li>
 *  <li>WAIT_ON_LOCK: lock journal on requested data, the concurrent users will wait for lock to be released</li>
 *  <li>NO_STRATEGY: lock journal on requested data, the concurrent users will fail during lock</li>
 * </ul>
 */
public enum ReadConcurrencyStrategy {
    NO_STRATEGY, WAIT_ON_LOCK, FAIL_ON_LOCK
}
