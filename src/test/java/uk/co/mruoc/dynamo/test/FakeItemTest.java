package uk.co.mruoc.dynamo.test;

import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import org.junit.Test;
import uk.co.mruoc.dynamo.test.FakeItem;

import static org.assertj.core.api.AssertionsForClassTypes.*;

public class FakeItemTest {

    @Test
    public void shouldReturnIdAsOneByDefault() {
        PrimaryKey expectedId = new PrimaryKey("id", "1");

        FakeItem item = new FakeItem();

        assertThat(item.getId().toString()).isEqualTo(expectedId.toString());
    }

    @Test
    public void shouldReturnBodyAsBodyValueByDefault() {
        FakeItem item = new FakeItem();

        assertThat(item.getBody()).isEqualTo("bodyValue");
    }

    @Test
    public void shouldReturnId() {
        String idValue = "2";
        String body = "customBody";
        PrimaryKey expectedId = new PrimaryKey("id", idValue);

        FakeItem item = new FakeItem(idValue, body);

        assertThat(item.getId().toString()).isEqualTo(expectedId.toString());
    }

    @Test
    public void shouldReturnIdAsString() {
        String idValue = "2";
        String body = "customBody";

        FakeItem item = new FakeItem(idValue, body);

        assertThat(item.getIdAsString()).isEqualTo(idValue);
    }

    @Test
    public void shouldReturnBody() {
        String idValue = "2";
        String body = "customBody";

        FakeItem item = new FakeItem(idValue, body);

        assertThat(item.getBody()).isEqualTo(body);
    }

}
