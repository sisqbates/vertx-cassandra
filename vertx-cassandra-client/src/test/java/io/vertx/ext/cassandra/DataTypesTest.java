package io.vertx.ext.cassandra;

import static java.util.stream.Collectors.joining;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Test;

public class DataTypesTest extends CassandraTestBase {

    private static final List<String> DATA_TYPES = Arrays.asList("ascii", "bigint", "blob", "boolean", "decimal",
            "double", "float", "inet", "int", "text", "timestamp", "uuid", "timeuuid", "varchar", "varint");

    private static final String COUNTER_TYPE = "counter";
    private static final List<String> COLLECTION_TYPES = Arrays.asList("list", "map", "set");

    private String updateStatement;
    private String selectStatement;

    private List<Object> params;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        String create = DATA_TYPES.stream().map(s -> "col_" + s + " " + s)
                .collect(joining(", ", "create table data_types(id int, col int, ", ", primary key(id))"));

        updateStatement = DATA_TYPES.stream().map(s -> "col_" + s + " = ?")
                .collect(joining(", ", "update data_types set ", ", col = 0 where id = 1"));
        selectStatement = DATA_TYPES.stream().map(s -> "col_" + s)
                .collect(joining(", ", "select ", " from data_types"));

        this.executeAndWait(1, h -> {
            cassandra.execute(create, h);
        });

        params = Arrays.asList(Arrays.copyOf(new Object[0], DATA_TYPES.size()));
    }

    @Test
    public void nulls() {

        this.updateAndSelect(r -> {
            this.assertTrue(r.getValues().get(0).stream().allMatch(c -> c == null));
            this.testComplete();
        });

        this.await();
    }

    @Test
    public void ascii() {
        String dataType = "ascii";
        Object value = "T";
        this.setColumnParam(dataType, value);

        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void bigint() {
        String dataType = "bigint";
        Object value = 12312321312L;
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void blob() {
        String dataType = "blob";
        Object value = "This is a blob";
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            ByteBuffer actualValue = (ByteBuffer) r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));

            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void booleanType() {
        String dataType = "boolean";
        Object value = Boolean.TRUE;
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void decimal() {
        String dataType = "decimal";
        Object value = new BigDecimal("4432432432423.231312321");
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void doubleType() {
        String dataType = "double";
        Object value = 31321321321d;
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void floatType() {
        String dataType = "float";
        Object value = 21312321312f;
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void inet() throws UnknownHostException {
        String dataType = "inet";
        Object value = InetAddress.getByName("192.168.1.1");
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void intType() {
        String dataType = "int";
        Object value = 2339123;
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void text() {
        String dataType = "text";
        Object value = "This is a text";
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void timestamp() {
        String dataType = "timestamp";
        Object value = new Date();
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void uuid() {
        String dataType = "uuid";
        Object value = UUID.randomUUID();
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void timeuuid() {
        String dataType = "timeuuid";
        Object value = UUID.fromString("a4a70900-24e1-11df-8924-001ff3591711");
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void varchar() {
        String dataType = "varchar";
        Object value = "this is a varchar";
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    @Test
    public void varint() {
        String dataType = "varint";
        Object value = new BigInteger("21312312312900");
        this.setColumnParam(dataType, value);
        this.updateAndSelect(r -> {
            Object actualValue = r.getValues().get(0).get(DATA_TYPES.indexOf(dataType));
            this.assertEquals(value.getClass(), actualValue.getClass());
            this.assertEquals(value, actualValue);
            this.testComplete();
        });
        this.await();
    }

    private void updateAndSelect(Consumer<ResultSet> handler) {
        cassandra.execute(updateStatement, params, this.onSuccess(r -> {
            cassandra.execute(selectStatement, this.onSuccess(r2 -> {
                this.assertEquals(1, r2.size());
                this.assertEquals(DATA_TYPES.size(), r2.getValues().get(0).size());
                handler.accept(r2);
            }));
        }));
    }

    private void setColumnParam(String dataType, Object value) {
        params.set(DATA_TYPES.indexOf(dataType), value);
    }

}
