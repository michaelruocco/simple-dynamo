package uk.co.mruoc.dynamo;

public class TableConfig {

    private final String tableName;
    private final String idFieldName;
    private final long readCapacityUnits;
    private final long writeCapacityUnits;

    public TableConfig(TableConfigBuilder builder) {
        this.tableName = builder.tableName;
        this.idFieldName = builder.idFieldName;
        this.readCapacityUnits = builder.readCapacityUnits;
        this.writeCapacityUnits = builder.writeCapacityUnits;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIdFieldName() {
        return idFieldName;
    }

    public long getReadCapacityUnits() {
        return readCapacityUnits;
    }

    public long getWriteCapacityUnits() {
        return writeCapacityUnits;
    }

    public static class TableConfigBuilder {

        private static final long DEFAULT_READ_CAPACITY_UNITS = 1;
        private static final long DEFAULT_WRITE_CAPACITY_UNITS = 1;

        private String tableName;
        private String idFieldName;
        private long readCapacityUnits = DEFAULT_READ_CAPACITY_UNITS;
        private long writeCapacityUnits = DEFAULT_WRITE_CAPACITY_UNITS;

        public TableConfigBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public TableConfigBuilder setIdFieldName(String idFieldName) {
            this.idFieldName = idFieldName;
            return this;
        }

        public TableConfigBuilder setReadCapacityUnits(long readCapacityUnits) {
            this.readCapacityUnits = readCapacityUnits;
            return this;
        }

        public TableConfigBuilder setWriteCapacityUnits(long writeCapacityUnits) {
            this.writeCapacityUnits = writeCapacityUnits;
            return this;
        }

        public TableConfig build() {
            return new TableConfig(this);
        }

    }

}
