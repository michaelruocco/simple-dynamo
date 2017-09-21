package uk.co.mruoc.dynamo.test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import org.junit.Test;
import uk.co.mruoc.dynamo.Items;

import static org.assertj.core.api.Assertions.assertThat;

public class FakeTableTest {

    private final FakeTable table = new FakeTable();

    @Test
    public void shouldNotBeCreatedByDefault() {
        assertThat(table.getTimesCreateCalled()).isEqualTo(0);
        assertThat(table.isCreated()).isFalse();
    }

    @Test
    public void shouldBeCreatedWhenCreateCalled() {
        table.create();

        assertThat(table.getTimesCreateCalled()).isEqualTo(1);
        assertThat(table.isCreated()).isTrue();
    }

    @Test
    public void shouldRecordNumberOfTimesCreated() {
        table.create();
        table.create();
        table.create();

        assertThat(table.getTimesCreateCalled()).isEqualTo(3);
    }

    @Test
    public void shouldNotBeClearedByDefault() {
        assertThat(table.getTimesClearCalled()).isEqualTo(0);
        assertThat(table.isCleared()).isFalse();
    }

    @Test
    public void shouldBeClearedWhenCreateCalled() {
        table.clear();

        assertThat(table.getTimesClearCalled()).isEqualTo(1);
        assertThat(table.isCleared()).isTrue();
    }

    @Test
    public void shouldRecordNumberOfTimesCleared() {
        table.clear();
        table.clear();
        table.clear();

        assertThat(table.getTimesClearCalled()).isEqualTo(3);
    }

    @Test
    public void shouldReturnLastDeletedItem() {
        FakeItem item = new FakeItem();

        table.delete(item);

        assertThat(table.getLastDeletedItem()).isEqualTo(item);
    }

    @Test
    public void shouldReturnSpecifiedItemsForGetAll() {
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        table.setItemsToReturnForAll(item1, item2);

        Items items = table.getAll();

        assertThat(items).containsExactly(item1, item2);
    }

    @Test
    public void shouldReturnSpecifiedItemsForGetAllWithStartKey() {
        String startKey = "start-key";
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        table.setItemsToReturnForAll(item1, item2);

        Items items = table.getAll(startKey);

        assertThat(items).containsExactly(item1, item2);
    }

    @Test
    public void shouldReturnGetAllStartKey() {
        String startKey = "start-key";

        table.getAll(startKey);

        assertThat(table.getLastStartKey()).isEqualTo(startKey);
    }

    @Test
    public void shouldReturnItemIfSetItemToReturnWithIdFieldName() {
        FakeItem expectedItem = new FakeItem("my-id", "{}");
        table.setItemToReturnForId(expectedItem.getIdAsString(), expectedItem);

        Item item = table.get(expectedItem.getIdAsString());

        assertThat(item).isEqualTo(expectedItem);
    }

    @Test(expected = FakeTableException.class)
    public void shouldThrowExceptionIfSetItemToReturnWithIdDoesNotMatchIdFieldName() {
        FakeItem expectedItem = new FakeItem("my-id", "{}");
        PrimaryKey key = new PrimaryKey("id", "my-id");
        table.setIdFieldName("another-id-name");
        table.setItemToReturnForId(expectedItem.getIdAsString(), expectedItem);

        table.get(key);
    }

    @Test
    public void shouldReturnItemForId() {
        FakeItem expectedItem = new FakeItem("my-id", "{}");
        table.setIdFieldName("id");
        table.setItemToReturnForId(expectedItem.getIdAsString(), expectedItem);

        Item item = table.get(expectedItem.getIdAsString());

        assertThat(item).isEqualTo(expectedItem);
    }

    @Test
    public void shouldReturnLastWrittenItem() {
        FakeItem item = new FakeItem("my-id", "{}");

        table.write(item);

        assertThat(table.getLastWrittenItem()).isEqualTo(item);
    }

}
