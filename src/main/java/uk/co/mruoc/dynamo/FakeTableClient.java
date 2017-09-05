package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.*;
import uk.co.mruoc.dynamo.Items.ItemsBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FakeTableClient implements TableClient {

    private TableConfig lastWrittenTableConfig;
    private Item lastWrittenItem;

    private TableConfig lastDeletedTableConfig;
    private Item lastDeletedItem;

    private TableConfig lastCreatedTableConfig;
    private TableConfig lastClearedTableConfig;
    private TableConfig lastReadTableConfig;

    private ItemCollection<ScanOutcome> allItems = new FakeItemCollection();
    private String lastStartKey;

    private Map<String, Item> itemsToReturn = new HashMap<>();

    @Override
    public void createTable(TableConfig tableConfig) {
        this.lastCreatedTableConfig = tableConfig;
    }

    @Override
    public void clear(TableConfig tableConfig) {
        this.lastClearedTableConfig = tableConfig;
    }

    @Override
    public void delete(TableConfig tableConfig, Item item) {
        this.lastDeletedTableConfig = tableConfig;
        this.lastDeletedItem = item;
    }

    @Override
    public Items getAll(TableConfig tableConfig) {
        this.lastReadTableConfig = tableConfig;
        return new ItemsBuilder().setScanOutcomes(allItems).build();
    }

    @Override
    public Items getAll(TableConfig tableConfig, String startKey) {
        this.lastStartKey = startKey;
        return getAll(tableConfig);
    }

    @Override
    public Item get(TableConfig tableConfig, String id) {
        PrimaryKey key = buildKey(tableConfig, id);
        return get(tableConfig, key);
    }

    @Override
    public Item get(TableConfig tableConfig, PrimaryKey key) {
        this.lastWrittenTableConfig = tableConfig;
        String id = extractId(key);
        return itemsToReturn.get(id);
    }

    @Override
    public void write(TableConfig tableConfig, Item item) {
        this.lastWrittenTableConfig = tableConfig;
        this.lastWrittenItem = item;
    }

    public void setItemsToReturnForAll(Item... items) {
        this.allItems = new FakeItemCollection(items);
    }

    public void setItemToReturnForId(String id, Item itemToReturn) {
        itemsToReturn.put(id, itemToReturn);
    }

    public TableConfig getLastWrittenTableConfig() {
        return lastWrittenTableConfig;
    }

    public Item getLastWrittenItem() {
        return lastWrittenItem;
    }

    public TableConfig getLastDeletedTableConfig() {
        return lastDeletedTableConfig;
    }

    public Item getLastDeletedItem() {
        return lastDeletedItem;
    }

    public TableConfig getLastCreatedTableConfig() {
        return lastCreatedTableConfig;
    }

    public TableConfig getLastClearedTableConfig() {
        return lastClearedTableConfig;
    }

    public TableConfig getLastReadTableConfig() {
        return lastReadTableConfig;
    }

    public String getLastStartKey() {
        return lastStartKey;
    }

    private PrimaryKey buildKey(TableConfig tableConfig, String value) {
        return new PrimaryKey(tableConfig.getIdFieldName(), value);
    }

    private String extractId(PrimaryKey key) {
        Iterator<KeyAttribute> attributes = key.getComponents().iterator();
        while(attributes.hasNext()) {
            KeyAttribute attribute = attributes.next();
            if (attribute.getName().equals(lastWrittenTableConfig.getIdFieldName())) {
                return attribute.getValue().toString();
            }
        }
        throw new FakeSimpleTableException("id attribute not found for primary key " + key);
    }

}
