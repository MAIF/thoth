package fr.maif.eventsourcing.impl;

public class TableNames {

    public final String tableName;
    /**
     * @deprecated not used since journal ID are now UUID
     */
    @Deprecated
    public final String sequenceIdName;
    public final String sequenceNumName;

    /**
     * @deprecated sequenceIdName is not used anymore for id generation
     */
    @Deprecated
    public TableNames(String tableName, String sequenceIdName, String sequenceNumName) {
        this.tableName = tableName;
        this.sequenceIdName = sequenceIdName;
        this.sequenceNumName = sequenceNumName;
    }

    public TableNames(String tableName, String sequenceNumName) {
        this.tableName = tableName;
        this.sequenceNumName = sequenceNumName;
        this.sequenceIdName = null;
    }
}
