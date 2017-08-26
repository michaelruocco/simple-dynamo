package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.junit.Test;
import uk.co.mruoc.dynamo.Items.ItemsBuilder;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ItemsTest {

    private final ItemsBuilder builder = new ItemsBuilder();

    @Test
    public void shouldReturnLastEvaluatedKey() {
        String lastKey = "my-last-key";

        Items items = builder.setLastEvaluatedKey(lastKey).build();

        assertThat(items.getLastEvaluatedKey()).isEqualTo(lastKey);
    }

    @Test
    public void hasMorePagesShouldBeTrueIfLastEvaluatedKeySet() {
        String lastKey = "my-last-key";

        Items items = builder.setLastEvaluatedKey(lastKey).build();

        assertThat(items.hasMorePages()).isTrue();
    }

    @Test
    public void hasMorePagesShouldBeFalseIfLastEvaluatedKeyNotSet() {
        Items items = builder.build();

        assertThat(items.hasMorePages()).isFalse();
    }

    @Test
    public void shouldSetListOfItems() {
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        List<Item> itemList = Arrays.asList(item1, item2);

        Items items = builder.setItems(itemList).build();

        Iterator<Item> iterator = items.iterator();
        assertThat(iterator.next()).isEqualTo(item1);
        assertThat(iterator.next()).isEqualTo(item2);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnItemCount() {
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        List<Item> itemList = Arrays.asList(item1, item2);

        Items items = builder.setItems(itemList).build();

        assertThat(items.getCount()).isEqualTo(2);
    }

    @Test
    public void shouldReturnItemsAsJsonArray() {
        String expectedJson = "[{\"id\":\"1\",\"body\":\"bodyValue\"},{\"id\":\"1\",\"body\":\"bodyValue\"}]";
        FakeItem item1 = new FakeItem();
        FakeItem item2 = new FakeItem();
        List<Item> itemList = Arrays.asList(item1, item2);

        Items items = builder.setItems(itemList).build();

        assertThat(items.toJsonArray()).isEqualTo(expectedJson);
    }

}
