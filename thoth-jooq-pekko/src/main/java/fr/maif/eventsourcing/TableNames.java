package fr.maif.eventsourcing;

public class TableNames {

    public final String tableName;
    public final String sequenceNumName;

    public TableNames(String tableName, String sequenceNumName) {
        this.tableName = tableName;
        this.sequenceNumName = sequenceNumName;
    }
}
