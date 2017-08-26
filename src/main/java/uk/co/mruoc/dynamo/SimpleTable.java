package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;

public interface SimpleTable {

    void createTable(TableConfig tableConfig);

    void clear(TableConfig tableConfig);

    void delete(TableConfig tableConfig, Item item);

    Items getAll(TableConfig tableConfig);

    Items getAll(TableConfig tableConfig, String startKey);

    Item get(TableConfig tableConfig, String id);

    Item get(TableConfig tableConfig, PrimaryKey key);

    void write(TableConfig tableConfig, Item item);

}
