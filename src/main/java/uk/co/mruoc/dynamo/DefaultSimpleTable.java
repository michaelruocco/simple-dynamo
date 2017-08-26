package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import uk.co.mruoc.dynamo.Items.ItemsBuilder;
import uk.co.mruoc.log.Logger;
import uk.co.mruoc.log.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;

public class DefaultSimpleTable implements SimpleTable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSimpleTable.class);

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDB dynamoDB;

    public DefaultSimpleTable(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
    }

    @Override
    public void createTable(TableConfig config) {
        String idFieldName = config.getIdFieldName();
        AttributeDefinition attributeDefinition = new AttributeDefinition(idFieldName, ScalarAttributeType.S);
        List<AttributeDefinition> attributeDefinitions = Collections.singletonList(attributeDefinition);
        List<KeySchemaElement> ks = Collections.singletonList(new KeySchemaElement(idFieldName, HASH));
        ProvisionedThroughput provisionedThroughput = toProvisionedThroughput(config);
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(config.getTableName())
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(ks)
                .withProvisionedThroughput(provisionedThroughput);
        amazonDynamoDB.createTable(request);
    }

    @Override
    public void clear(TableConfig tableConfig) {
        String tableName = tableConfig.getTableName();
        Table table = getTable(tableName);
        String idFieldName = tableConfig.getIdFieldName();
        ScanSpec scanSpec = new ScanSpec().withProjectionExpression(idFieldName);
        ItemCollection<ScanOutcome> outcomes = table.scan(scanSpec);
        for (Item item : outcomes)
            delete(tableConfig, item);
    }

    @Override
    public void delete(TableConfig tableConfig, Item item) {
        Table table = getTable(tableConfig.getTableName());
        delete(tableConfig, table, item);
    }

    @Override
    public Items getAll(TableConfig tableConfig) {
        String tableName = tableConfig.getTableName();
        Table table = getTable(tableName);
        ScanSpec scanSpec = new ScanSpec();
        ItemCollection<ScanOutcome> outcomes = table.scan(scanSpec);
        return new ItemsBuilder().setScanOutcomes(outcomes).build();
    }

    @Override
    public Items getAll(TableConfig tableConfig, String startKey) {
        String tableName = tableConfig.getTableName();
        Table table = getTable(tableName);
        PrimaryKey primaryKey = new PrimaryKey(tableConfig.getIdFieldName(), startKey);
        ScanSpec scanSpec = new ScanSpec().withExclusiveStartKey(primaryKey);
        ItemCollection<ScanOutcome> outcomes = table.scan(scanSpec);
        return new ItemsBuilder().setScanOutcomes(outcomes).build();
    }

    @Override
    public Item get(TableConfig tableConfig, String id) {
        PrimaryKey key = buildKey(tableConfig, id);
        return get(tableConfig, key);
    }

    @Override
    public Item get(TableConfig tableConfig, PrimaryKey key) {
        String tableName = tableConfig.getTableName();
        Table table = getTable(tableName);
        return table.getItem(key);
    }

    @Override
    public void write(TableConfig tableConfig, Item item) {
        String tableName = tableConfig.getTableName();
        Table table = getTable(tableName);
        table.putItem(item);
    }

    private ProvisionedThroughput toProvisionedThroughput(TableConfig config) {
        return new ProvisionedThroughput(config.getReadCapacityUnits(), config.getWriteCapacityUnits());
    }

    private void delete(TableConfig tableConfig, Table table, Item item) {
        String id = item.get(tableConfig.getIdFieldName()).toString();
        PrimaryKey key = buildKey(tableConfig, id);
        table.deleteItem(key);
    }

    private PrimaryKey buildKey(TableConfig tableConfig, String value) {
        return new PrimaryKey(tableConfig.getIdFieldName(), value);
    }

    private Table getTable(String tableName) {
        return dynamoDB.getTable(tableName);
    }

}
