package io.vertx.ext.cassandra;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class StatementTest extends CassandraTestBase {

    private static final String CREATE_SIMPLE_TABLE = "create table simple_table (pk varchar, ck int, primary key ((pk), ck))";
    private static final String INSERT_TEMPLATE = "insert into simple_table (pk, ck) values ('%s', %d)";
    private static final int NUM_INSERTS = 50;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.executeAndWait(1, h -> {
            cassandra.execute(CREATE_SIMPLE_TABLE, h);
        });

    }

    private void insertValues() throws InterruptedException {

        this.executeAndWait(NUM_INSERTS, h -> {
            for (int i = 0; i < NUM_INSERTS; i++) {
                cassandra.execute(String.format(INSERT_TEMPLATE, Integer.toString(i / 10), i), h);
            }
        });

    }

    @Test
    public void simpleQuery() throws InterruptedException {

        this.insertValues();

        cassandra.execute("select * from simple_table where pk = '1'", this.onSuccess(result -> {
            this.assertEquals(10, result.size());

            List<List<Object>> values = result.getValues();
            for (int i = 0; i < 10; i++) {
                List<Object> row = values.get(i);
                this.assertEquals(2, row.size());
                this.assertEquals(i + 10, (int) row.get(1));
            }
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void parameterizedQuery() throws InterruptedException {

        this.insertValues();

        cassandra.execute("select * from simple_table where pk = ?", Arrays.asList("1"), this.onSuccess(result -> {
            this.assertEquals(10, result.size());
            List<List<Object>> values = result.getValues();
            for (int i = 0; i < 10; i++) {
                List<Object> row = values.get(i);
                this.assertEquals(2, row.size());
                this.assertEquals(i + 10, row.get(1));
            }
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void simpleInsert() throws InterruptedException {
        cassandra.execute("insert into simple_table (pk, ck) values ('42', 36)", this.onSuccess(result -> {
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void parameterizedInsert() throws InterruptedException {
        cassandra.execute("insert into simple_table (pk, ck) values (?, 36)", Arrays.asList("42"),
                this.onSuccess(result -> {
                    this.testComplete();
                }));
        this.await();
    }

}

// - ResultSetSerialization
// - see CQL data types and test each of the types
