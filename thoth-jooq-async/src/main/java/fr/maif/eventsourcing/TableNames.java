package fr.maif.eventsourcing;

public class TableNames {

    public final String tableName;
    public final String sequenceNumName;
    public final String lockTableName;

    public TableNames(String tableName, String sequenceNumName) {
        this.tableName = tableName;
        this.sequenceNumName = sequenceNumName;
        this.lockTableName = null;
    }

    public TableNames(String tableName, String sequenceNumName, String lockTableName) {
        this.tableName = tableName;
        this.sequenceNumName = sequenceNumName;
        this.lockTableName = lockTableName;
    }
}
