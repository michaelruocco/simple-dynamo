package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.junit.Test;
import uk.co.mruoc.dynamo.TableConfig.TableConfigBuilder;

import static org.assertj.core.api.Assertions.assertThat;

public class FakeSimpleTableTest {

    private final FakeSimpleTable table = new FakeSimpleTable();

    private final FakeItem item = new FakeItem();
    private final TableConfig config = new TableConfigBuilder()
            .setIdFieldName("id")
            .build();

    @Test
    public void shouldReturnLastCreatedTableConfig() {
        table.createTable(config);

        assertThat(table.getLastCreatedTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnLastClearedTableConfig() {
        table.clear(config);

        assertThat(table.getLastClearedTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnLastDeletedTableConfig() {
        table.delete(config, item);

        assertThat(table.getLastDeletedTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnLastDeletedItem() {
        table.delete(config, item);

        assertThat(table.getLastDeletedItem()).isEqualTo(item);
    }

    @Test
    public void shouldReturnLastReadTableConfigForGetAll() {
        table.getAll(config);

        assertThat(table.getLastReadTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnSpecifiedItemsFromGetAll() {
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        table.setItemsToReturnForAll(item1, item2);

        Items items = table.getAll(config);

        assertThat(items).containsExactly(item1, item2);
    }

    @Test
    public void shouldReturnLastReadTableConfigForPagedGetAll() {
        String startKey = "startKey";

        table.getAll(config, startKey);

        assertThat(table.getLastReadTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnSpecifiedItemsFromPagedGetAll() {
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        table.setItemsToReturnForAll(item1, item2);
        String startKey = "startKey";

        Items items = table.getAll(config, startKey);

        assertThat(items).containsExactly(item1, item2);
    }

    @Test
    public void shouldReturnLastStartKeyFromPagedGetAll() {
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        table.setItemsToReturnForAll(item1, item2);
        String startKey = "startKey";

        table.getAll(config, startKey);

        assertThat(table.getLastStartKey()).isEqualTo(startKey);
    }

    @Test
    public void shouldReturnSpecifiedItem() {
        FakeItem expectedItem = new FakeItem();
        table.setItemToReturnForId(expectedItem.getIdAsString(), expectedItem);

        Item item = table.get(config, expectedItem.getIdAsString());

        assertThat(item).isEqualTo(expectedItem);
    }

    @Test
    public void shouldReturnNullIfItemToReturnNotSpecified() {
        FakeItem expectedItem = new FakeItem();

        Item item = table.get(config, expectedItem.getIdAsString());

        assertThat(item).isNull();
    }

    @Test
    public void shouldReturnLastWrittenTableConfig() {
        table.write(config, item);

        assertThat(table.getLastWrittenTableConfig()).isEqualTo(config);
    }

    @Test
    public void shouldReturnLastWrittenItem() {
        FakeItem item = new FakeItem();

        table.write(config, item);

        assertThat(table.getLastWrittenItem()).isEqualTo(item);
    }


}
