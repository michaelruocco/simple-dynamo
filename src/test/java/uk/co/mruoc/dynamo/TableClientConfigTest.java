package uk.co.mruoc.dynamo;

import org.junit.Test;
import uk.co.mruoc.dynamo.TableConfig.TableConfigBuilder;

import static org.assertj.core.api.Assertions.assertThat;

public class TableClientConfigTest {

    private TableConfigBuilder builder = new TableConfigBuilder();

    @Test
    public void shouldReturnTableName() {
        String tableName = "table-name";

        TableConfig config = builder.setTableName(tableName).build();

        assertThat(config.getTableName()).isEqualTo(tableName);
    }

    @Test
    public void shouldReturnIdFieldName() {
        String idFieldName = "id-field-name";

        TableConfig config = builder.setIdFieldName(idFieldName).build();

        assertThat(config.getIdFieldName()).isEqualTo(idFieldName);
    }

    @Test
    public void shouldReturnOneAsDefaultReadCapacityUnits() {
        TableConfig config = builder.build();

        assertThat(config.getReadCapacityUnits()).isEqualTo(1L);
    }

    @Test
    public void shouldReturnReadCapacityUnits() {
        long readCapacityUnits = 5;

        TableConfig config = builder.setReadCapacityUnits(readCapacityUnits).build();

        assertThat(config.getReadCapacityUnits()).isEqualTo(readCapacityUnits);
    }

    @Test
    public void shouldReturnOneAsDefaultWriteCapacityUnits() {
        TableConfig config = builder.build();

        assertThat(config.getWriteCapacityUnits()).isEqualTo(1L);
    }

    @Test
    public void shouldReturnWriteCapacityUnits() {
        long writeCapacityUnits = 5;

        TableConfig config = builder.setWriteCapacityUnits(writeCapacityUnits).build();

        assertThat(config.getWriteCapacityUnits()).isEqualTo(writeCapacityUnits);
    }

}
