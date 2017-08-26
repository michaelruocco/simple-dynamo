package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import org.slf4j.Logger;

public class LogSimpleTable extends DefaultSimpleTable {

    private final Logger logger;

    public LogSimpleTable(AmazonDynamoDB amazonDynamoDB, Logger logger) {
        super(amazonDynamoDB);
        this.logger = logger;
    }

    @Override
    public void createTable(TableConfig config) {
        logger.info("creating table " + config.getTableName() + " with id field name " + config.getIdFieldName());
        super.createTable(config);
    }

    @Override
    public void clear(TableConfig config) {
        logger.info("clearing table " + config.getTableName());
        super.clear(config);
    }

    @Override
    public Items getAll(TableConfig config) {
        logger.info("reading all items from table " + config.getTableName());
        return super.getAll(config);
    }

    @Override
    public Items getAll(TableConfig config, String startKey) {
        logger.info("reading all items from table " + config.getTableName());
        return super.getAll(config, startKey);
    }

    @Override
    public Item get(TableConfig config, PrimaryKey key) {
        logger.info("reading item with primary key " + key.toString() + " from table " + config.getTableName());
        Item item = super.get(config, key);
        logReadItem(item);
        return item;
    }

    @Override
    public void write(TableConfig config, Item item) {
        logger.info("writing item " + item.toJSON() + " to table" + config.getTableName());
        super.write(config, item);
    }

    @Override
    public void delete(TableConfig tableConfig, Item item) {
        logger.info("deleting item " + item.toJSON() + " from table " + tableConfig.getTableName());
        super.delete(tableConfig, item);
    }

    private void logReadItem(Item item) {
        if (item == null) {
            logger.info("item not found");
            return;
        }
        logger.info("read item " + item.toJSON());
    }

}
