package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;

public class DefaultDynamoTable implements DynamoTable {

    private final TableConfig config;
    private final TableClient table;

    public DefaultDynamoTable(TableConfig config, TableClient table) {
        this.config = config;
        this.table = table;
    }

    @Override
    public void create() {
        table.createTable(config);
    }

    @Override
    public void clear() {
        table.clear(config);
    }

    @Override
    public void delete(Item item) {
        table.delete(config, item);
    }

    @Override
    public Items getAll() {
        return table.getAll(config);
    }

    @Override
    public Items getAll(String startKey) {
        return table.getAll(config, startKey);
    }

    @Override
    public Item get(String id) {
        return table.get(config, id);
    }

    @Override
    public Item get(PrimaryKey key) {
        return table.get(config, key);
    }

    @Override
    public void write(Item item) {
        table.write(config, item);
    }

    @Override
    public String getIdFieldName() {
        return config.getIdFieldName();
    }

}
