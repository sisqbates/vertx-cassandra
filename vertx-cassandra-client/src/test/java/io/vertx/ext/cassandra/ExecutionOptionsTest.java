package io.vertx.ext.cassandra;

import java.util.Collections;

import org.junit.Test;

public class ExecutionOptionsTest extends CassandraTestBase {

    private void insertSomeValues(int rows) throws InterruptedException {
        this.executeAndWait(1, h -> {
            cassandra.execute("create table dummy(pk int, primary key(pk))", h);
        });
        this.executeAndWait(rows, h -> {
            for (int i = 0; i < rows; i++) {
                cassandra.execute("insert into dummy (pk) values (?)", Collections.singletonList(i), h);
            }
        });
    }

    @Test
    public void tracingDefault() throws InterruptedException {
        this.insertSomeValues(1);

        cassandra.execute("select * from dummy", this.onSuccess(r -> {
            this.assertNull(r.getMetaInformation().getQueryTrace());
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void tracingOn() throws InterruptedException {
        this.insertSomeValues(1);

        ExecutionOptions options = new ExecutionOptions().setTracing(true);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertNotNull(r.getMetaInformation().getQueryTrace());
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void tracingOff() throws InterruptedException {
        this.insertSomeValues(1);

        ExecutionOptions options = new ExecutionOptions().setTracing(false);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertNull(r.getMetaInformation().getQueryTrace());
            this.testComplete();
        }));
        this.await();

    }

    @Test
    public void defaultFetchSize() throws InterruptedException {
        this.insertSomeValues(5001);

        cassandra.execute("select * from dummy", this.onSuccess(r -> {
            this.assertEquals(5000, r.size());
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void fetchSizeSmaller() throws InterruptedException {
        this.insertSomeValues(50);

        ExecutionOptions options = new ExecutionOptions().setFetchSize(10);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertEquals(10, r.size());
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void fetchSizeBigger() throws InterruptedException {
        this.insertSomeValues(50);

        ExecutionOptions options = new ExecutionOptions().setFetchSize(100);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertEquals(50, r.size());
            this.testComplete();
        }));
        this.await();
    }

    // ExecutionOptionsTest
    // - consistencyLevel
    // - fetchSize & pagingState
    // - idempotent
    // - retryPolicy
    // - serialConsistencyLevel

}
