package uk.co.mruoc.dynamo.test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;

public class FakeItem extends Item {

    private static final String DEFAULT_ID_FIELD_NAME = "id";
    private static final String BODY_FIELD_NAME = "body";

    private final String idFieldName;

    public FakeItem() {
        this(DEFAULT_ID_FIELD_NAME, "1", "bodyValue");
    }

    public FakeItem(String id, String body) {
        this(DEFAULT_ID_FIELD_NAME, id, body);
    }

    public FakeItem(String idFieldName, String id, String body) {
        this.idFieldName = idFieldName;
        withPrimaryKey(idFieldName, id);
        withString(BODY_FIELD_NAME, body);
    }

    public PrimaryKey getId() {
        String id = getIdAsString();
        return new PrimaryKey(idFieldName, id);
    }

    public String getIdAsString() {
        return get(idFieldName).toString();
    }

    public String getBody() {
        return getString(BODY_FIELD_NAME);
    }

}
