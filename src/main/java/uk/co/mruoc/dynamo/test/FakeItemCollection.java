package uk.co.mruoc.dynamo.test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.Arrays;
import java.util.List;

public class FakeItemCollection extends ItemCollection<ScanOutcome> {

    private final Item[] items;

    public FakeItemCollection() {
        this(new Item[0]);
    }

    public FakeItemCollection(Item[] items) {
        this.items = items;
    }

    @Override
    public Integer getMaxResultSize() {
        return items.length;
    }

    @Override
    public Page<Item, ScanOutcome> firstPage() {
        List<Item> pageItems = Arrays.asList(items);
        return new FakePage(pageItems, new ScanOutcome(new ScanResult()));
    }

    private static final class FakePage extends Page<Item, ScanOutcome> {

        public FakePage(List<Item> content, ScanOutcome lowLevelResult) {
            super(content, lowLevelResult);
        }

        @Override
        public boolean hasNextPage() {
            return false;
        }

        @Override
        public Page<Item, ScanOutcome> nextPage() {
            return null;
        }

    }

}
