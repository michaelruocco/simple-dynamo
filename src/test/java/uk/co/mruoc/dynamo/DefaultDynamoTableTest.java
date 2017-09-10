package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import org.junit.Test;
import uk.co.mruoc.dynamo.TableConfig.TableConfigBuilder;
import uk.co.mruoc.dynamo.test.FakeItem;
import uk.co.mruoc.dynamo.test.FakeTableClient;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultDynamoTableTest {

    private static final String ID_FIELD_NAME = "id-field";
    private final TableConfig config = new TableConfigBuilder()
            .setIdFieldName(ID_FIELD_NAME)
            .build();
    private final FakeTableClient client = new FakeTableClient();

    private final DefaultDynamoTable table = new DefaultDynamoTable(config, client);

    @Test
    public void shouldUseConfigForCreate() {
        table.create();

        assertThat(client.getLastCreatedTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldUseConfigForClear() {
        table.clear();

        assertThat(client.getLastClearedTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldUseConfigForDelete() {
        table.delete(new FakeItem());

        assertThat(client.getLastDeletedTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldDeleteGivenItem() {
        FakeItem item = new FakeItem();

        table.delete(item);

        assertThat(client.getLastDeletedItem()).isEqualTo(item);
    }

    @Test
    public void shouldUseConfigForGetAll() {
        table.getAll();

        assertThat(client.getLastReadTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnItemsForGetAll() {
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        client.setItemsToReturnForAll(item1, item2);

        Items results = table.getAll();

        assertThat(results).containsExactly(item1, item2);
    }

    @Test
    public void shouldUseConfigForGetAllWithStartKey() {
        String startKey = "my-start-key";

        table.getAll(startKey);

        assertThat(client.getLastStartKey()).isEqualTo(startKey);
    }

    @Test
    public void shouldReturnItemsForGetAllWithStartKey() {
        String startKey = "my-start-key";
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        client.setItemsToReturnForAll(item1, item2);

        Items results = table.getAll(startKey);

        assertThat(results).containsExactly(item1, item2);
    }

    @Test
    public void shouldUseConfigForGetSingle() {
        String id = "my-id";

        table.get(id);

        assertThat(client.getLastWrittenTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnItemForGivenId() {
        String id = "my-id";
        FakeItem expectedItem = new FakeItem(id, "body");
        client.setItemToReturnForId(id, expectedItem);

        Item result = table.get(id);

        assertThat(result).isEqualTo(expectedItem);
    }

    @Test
    public void shouldReturnItemForGivenIdUsingPrimaryKey() {
        String id = "my-id";
        PrimaryKey key = new PrimaryKey(config.getIdFieldName(), id);
        FakeItem expectedItem = new FakeItem(id, "body");
        client.setItemToReturnForId(id, expectedItem);

        Item result = table.get(key);

        assertThat(result).isEqualTo(expectedItem);
    }

    @Test
    public void shouldUseConfigForWrite() {
        FakeItem item = new FakeItem();

        table.write(item);

        assertThat(client.getLastWrittenTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldWriteItem() {
        FakeItem item = new FakeItem();

        table.write(item);

        assertThat(client.getLastWrittenItem()).isEqualTo(item);
    }

    @Test
    public void shouldReturnIdFieldName() {
        assertThat(table.getIdFieldName()).isEqualTo(ID_FIELD_NAME);
    }


}
