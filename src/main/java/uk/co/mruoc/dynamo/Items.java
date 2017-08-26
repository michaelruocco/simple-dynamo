package uk.co.mruoc.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public class Items implements Iterable<Item> {

    private final List<Item> items;
    private final String lastEvaluatedKey;

    private Items(ItemsBuilder builder) {
        this.items = builder.items;
        this.lastEvaluatedKey = builder.lastEvaluatedKey;
    }

    public boolean hasMorePages() {
        return !lastEvaluatedKey.isEmpty();
    }

    public String getLastEvaluatedKey() {
        return lastEvaluatedKey;
    }

    public String toJsonArray() {
        List<String> json = new ArrayList<>();
        items.forEach(i -> json.add(i.toJSON()));
        return "[" + String.join(",", json) + "]";
    }

    public int getCount() {
        return items.size();
    }

    @Override
    public Iterator<Item> iterator() {
        return items.iterator();
    }

    public static class ItemsBuilder {

        private List<Item> items = new ArrayList<>();
        private String lastEvaluatedKey = EMPTY;

        public ItemsBuilder setScanOutcomes(ItemCollection<ScanOutcome> outcomes) {
            setItems(outcomes);
            setLastEvaluatedKey(outcomes);
            return this;
        }

        public ItemsBuilder setLastEvaluatedKey(String lastEvaluatedKey) {
            this.lastEvaluatedKey = lastEvaluatedKey;
            return this;
        }

        public ItemsBuilder setItems(List<Item> items) {
            this.items.clear();
            this.items.addAll(items);
            return this;
        }

        public Items build() {
            return new Items(this);
        }

        private void setItems(ItemCollection<ScanOutcome> outcomes) {
            items.clear();;
            outcomes.forEach(o -> items.add(o));
        }

        private void setLastEvaluatedKey(ItemCollection<ScanOutcome> outcomes) {
            ScanOutcome outcome = outcomes.getLastLowLevelResult();
            if (outcome == null)
                return;

            ScanResult scanResult = outcome.getScanResult();
            if (scanResult == null)
                return;

            Map<String, AttributeValue> lastEvaluatedKey = scanResult.getLastEvaluatedKey();
            if (lastEvaluatedKey == null)
                return;

            this.lastEvaluatedKey = lastEvaluatedKey.get("lastEvaluatedKey").getS();
        }

    }

}
