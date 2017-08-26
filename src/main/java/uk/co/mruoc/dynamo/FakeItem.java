package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;

public class FakeItem extends Item {

    private static final String ID_FIELD_NAME = "id";
    private static final String BODY_FIELD_NAME = "body";

    public FakeItem() {
        this("1", "bodyValue");
    }

    public FakeItem(String id, String body) {
        withPrimaryKey(ID_FIELD_NAME, id);
        withString(BODY_FIELD_NAME, body);
    }

    public PrimaryKey getId() {
        String id = getIdAsString();
        return new PrimaryKey(ID_FIELD_NAME, id);
    }

    public String getIdAsString() {
        return get(ID_FIELD_NAME).toString();
    }

    public String getBody() {
        return getString(BODY_FIELD_NAME);
    }

}
