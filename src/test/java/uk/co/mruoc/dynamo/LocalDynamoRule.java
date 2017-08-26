package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import org.junit.rules.ExternalResource;

public class LocalDynamoRule extends ExternalResource {

    private AmazonDynamoDB dynamoDb;

    public AmazonDynamoDB getAmazonDynamoDb() {
        return dynamoDb;
    }

    @Override
    protected void before() throws Throwable {
        dynamoDb = DynamoDBEmbedded.create().amazonDynamoDB();
    }

    @Override
    protected void after() {
        dynamoDb.shutdown();
    }

}
