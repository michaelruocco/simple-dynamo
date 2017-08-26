package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import uk.co.mruoc.dynamo.TableConfig.TableConfigBuilder;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultSimpleTableTest {

    private static final String TABLE_NAME = "testTable";
    private static final String ID_FIELD_NAME = "id";

    @Rule
    public final LocalDynamoRule localDynamoRule = new LocalDynamoRule();

    private final TableConfig tableConfig = new TableConfigBuilder()
            .setTableName(TABLE_NAME)
            .setIdFieldName(ID_FIELD_NAME)
            .build();

    private SimpleTable client;

    @Before
    public void setUp() {
        AmazonDynamoDB amazonDynamoDB = localDynamoRule.getAmazonDynamoDb();
        client = new DefaultSimpleTable(amazonDynamoDB);
        client.createTable(tableConfig);
    }

    @Test
    public void shouldWriteItemToTable() {
        FakeItem expectedItem = new FakeItem();

        client.write(tableConfig, expectedItem);

        Item resultItem = client.get(tableConfig, expectedItem.getIdAsString());
        assertThat(resultItem).isEqualToComparingFieldByField(expectedItem);
    }

    @Test
    public void shouldReturnNullIfItemDoesNotExist() {
        PrimaryKey key = new PrimaryKey(ID_FIELD_NAME, "idValue");

        Item result = client.get(tableConfig, key);

        assertThat(result).isNull();
    }

    @Test
    public void shouldClearAllItemsInTable() {
        client.write(tableConfig, new FakeItem("id1", "body1"));
        client.write(tableConfig, new FakeItem("id2", "body2"));
        client.write(tableConfig, new FakeItem("id3", "body3"));

        client.clear(tableConfig);

        assertThat(client.getAll(tableConfig).iterator().hasNext()).isEqualTo(false);
    }

    @Test
    public void shouldReturnAllItemsInTable() {
        FakeItem item1 = new FakeItem("id1", "body1");
        FakeItem item2 = new FakeItem("id2", "body2");
        FakeItem item3 = new FakeItem("id3", "body3");

        client.write(tableConfig, item1);
        client.write(tableConfig, item2);
        client.write(tableConfig, item3);

        Items items = client.getAll(tableConfig);

        assertThat(items).containsExactly(item1, item2, item3);
    }

    @Test
    public void shouldReturnAllItemsInTableWhenPaging() {
        FakeItem item1 = new FakeItem("id1", "body1");
        FakeItem item2 = new FakeItem("id2", "body2");
        FakeItem item3 = new FakeItem("id3", "body3");

        client.write(tableConfig, item1);
        client.write(tableConfig, item2);
        client.write(tableConfig, item3);

        Items items = client.getAll(tableConfig, "id1");

        assertThat(items).containsExactly(item2, item3);
    }

}
