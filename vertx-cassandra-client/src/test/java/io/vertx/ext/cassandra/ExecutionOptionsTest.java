package io.vertx.ext.cassandra;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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
            this.assertNull(r.getExecutionInfo().getQueryTrace());
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void tracingOn() throws InterruptedException {
        this.insertSomeValues(1);

        ExecutionOptions options = new ExecutionOptions().setTracing(true);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertNotNull(r.getExecutionInfo().getQueryTrace());
            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void tracingOff() throws InterruptedException {
        this.insertSomeValues(1);

        ExecutionOptions options = new ExecutionOptions().setTracing(false);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertNull(r.getExecutionInfo().getQueryTrace());
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

    // TODO: Think of how can we test consistency levels. The only way seems to
    // set up a cluster and bring up/down some of its nodes
    @Test
    public void defaultConsistencyLevel() throws InterruptedException {
        this.insertSomeValues(10);

        cassandra.execute("select * from dummy", this.onSuccess(r -> {
            this.assertNull(r.getExecutionInfo().getAchievedConsistencyLevel());

            this.testComplete();
        }));
        this.await();
    }

    @Test
    public void noPagingAvailable() throws InterruptedException {
        this.insertSomeValues(10);

        ExecutionOptions options = new ExecutionOptions().setFetchSize(100);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertNull(r.getExecutionInfo().getPagingState());
            this.testComplete();
        }));
        this.await();

    }

    @Test
    public void pagingAvailable() throws InterruptedException {
        this.insertSomeValues(5);

        ExecutionOptions options = new ExecutionOptions().setFetchSize(2);

        cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
            this.assertNotNull(r.getExecutionInfo().getPagingState());
            this.testComplete();
        }));
        this.await();

    }

    @Test
    public void numberOfPages() throws InterruptedException {
        this.insertSomeValues(10);

        ExecutionOptions options = new ExecutionOptions().setFetchSize(3);

        AtomicInteger count = new AtomicInteger(0);
        while (count.get() < 4) {
            CountDownLatch latch = new CountDownLatch(1);
            cassandra.executeWithOptions("select * from dummy", options, this.onSuccess(r -> {
                if (count.get() < 4) {
                    this.assertNotNull(r.getExecutionInfo().getPagingState());
                    options.setPagingState(r.getExecutionInfo().getPagingState());
                } else {
                    this.assertNull(r.getExecutionInfo().getPagingState());
                    this.testComplete();
                }
                latch.countDown();
            }));
            count.incrementAndGet();
            latch.await();
        }

        this.await();
    }

    // ExecutionOptionsTest
    // - defaultTimestamp
    // - idempotent
    // - retryPolicy
    // - serialConsistencyLevel

}
