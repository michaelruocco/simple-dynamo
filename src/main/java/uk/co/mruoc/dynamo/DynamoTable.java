package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;

public interface DynamoTable {

    void create();

    void clear();

    void delete(Item item);

    Items getAll();

    Items getAll(String startKey);

    Item get(String id);

    Item get(PrimaryKey key);

    void write(Item item);

    String getIdFieldName();

}
