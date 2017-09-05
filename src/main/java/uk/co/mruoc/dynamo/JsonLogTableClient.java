package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import uk.co.mruoc.log.Logger;

public class JsonLogTableClient extends DefaultTableClient {

    private final Logger logger;

    public JsonLogTableClient(AmazonDynamoDB amazonDynamoDB, Logger logger) {
        super(amazonDynamoDB);
        this.logger = logger;
    }

    @Override
    public void createTable(TableConfig config) {
        logger.info().message("creating table").field("tableConfig", config).log();
        super.createTable(config);
    }

    @Override
    public void clear(TableConfig config) {
        logger.info().message("clearing table").field("table", config).log();
        super.clear(config);
    }

    @Override
    public Items getAll(TableConfig config) {
        logger.info().message("reading all items").field("tableConfig", config).log();
        return super.getAll(config);
    }

    @Override
    public Items getAll(TableConfig config, String startKey) {
        logger.info().message("reading all items").field("tableConfig", config).field("startKey", startKey).log();
        return super.getAll(config, startKey);
    }

    @Override
    public Item get(TableConfig config, PrimaryKey key) {
        logger.info().message("reading item").field("primaryKey", key).field("tableConfig", config).log();
        Item item = super.get(config, key);
        logReadItem(item);
        return item;
    }

    @Override
    public void write(TableConfig config, Item item) {
        logger.info().message("writing item").field("item", item).field("tableConfig", config).log();
        super.write(config, item);
    }

    @Override
    public void delete(TableConfig tableConfig, Item item) {
        logger.info().message("deleting item").field("tableConfig", tableConfig).field("item", item).log();
        super.delete(tableConfig, item);
    }

    private void logReadItem(Item item) {
        if (item == null) {
            logger.info().message("item not found").log();
            return;
        }
        logger.info().message("read item").field("item", item).log();
    }

}
