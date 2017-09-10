package uk.co.mruoc.dynamo.test;

import com.amazonaws.services.dynamodbv2.document.*;
import uk.co.mruoc.dynamo.DynamoTable;
import uk.co.mruoc.dynamo.Items;
import uk.co.mruoc.dynamo.Items.ItemsBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FakeTable implements DynamoTable {

    private static final String DEFAULT_ID_FIELD_NAME = "ID";

    private String idFieldName = DEFAULT_ID_FIELD_NAME;
    private String lastStartKey;
    private Item lastDeletedItem;
    private Item lastWrittenItem;
    private int timesCreateCalled;
    private int timesClearCalled;

    private ItemCollection<ScanOutcome> allItems = new FakeItemCollection();
    private Map<String, Item> itemsToReturn = new HashMap<>();

    @Override
    public void create() {
        timesCreateCalled++;
    }

    @Override
    public void clear() {
        timesClearCalled++;
    }

    @Override
    public void delete(Item item) {
        this.lastDeletedItem = item;
    }

    @Override
    public Items getAll() {
        return new ItemsBuilder().setScanOutcomes(allItems).build();
    }

    @Override
    public Items getAll(String startKey) {
        this.lastStartKey = startKey;
        return getAll();
    }

    @Override
    public Item get(String id) {
        PrimaryKey key = buildKey(id);
        return get(key);
    }

    @Override
    public Item get(PrimaryKey key) {
        String id = extractId(key);
        return itemsToReturn.get(id);
    }

    @Override
    public void write(Item item) {
        this.lastWrittenItem = item;
    }

    @Override
    public String getIdFieldName() {
        return idFieldName;
    }

    public void setIdFieldName(String idFieldName) {
        this.idFieldName = idFieldName;
    }

    public String getLastStartKey() {
        return lastStartKey;
    }

    public Item getLastDeletedItem() {
        return lastDeletedItem;
    }

    public Item getLastWrittenItem() {
        return lastWrittenItem;
    }

    public void setItemsToReturnForAll(Item... items) {
        this.allItems = new FakeItemCollection(items);
    }

    public void setItemToReturnForId(String id, Item itemToReturn) {
        itemsToReturn.put(id, itemToReturn);
    }

    public int getTimesCreateCalled() {
        return timesCreateCalled;
    }

    public boolean isCreated() {
        return timesCreateCalled > 0;
    }

    public int getTimesClearCalled() {
        return timesClearCalled;
    }

    public boolean isCleared() {
        return timesClearCalled > 0;
    }

    private PrimaryKey buildKey(String value) {
        return new PrimaryKey(getIdFieldName(), value);
    }

    private String extractId(PrimaryKey key) {
        Iterator<KeyAttribute> attributes = key.getComponents().iterator();
        while(attributes.hasNext()) {
            KeyAttribute attribute = attributes.next();
            if (attribute.getName().equals("id")) {
                return attribute.getValue().toString();
            }
        }
        throw new FakeTableException("id attribute not found for primary key " + key);
    }

}
