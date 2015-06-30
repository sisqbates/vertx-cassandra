package io.vertx.ext.cassandra;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class ExecutionOptionsTest extends CassandraTestBase {

    private void insertSomeValues(int rows) throws InterruptedException {
        this.insertSomeValues(rows, null);
    }

    private void insertSomeValues(int rows, ExecutionOptions options) throws InterruptedException {
        this.executeAndWait(1, h -> {
            cassandra.execute("create table dummy(pk int, val int, primary key(pk))", h);
        });
        this.executeAndWait(rows, h -> {
            for (int i = 0; i < rows; i++) {
                cassandra.executeWithOptions("insert into dummy (pk, val) values (?, ?)", Arrays.asList(i, i), options,
                        h);
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

    @Test
    public void noTimestamp() throws InterruptedException {

        this.insertSomeValues(1);

        // Delete the row
        cassandra.execute("delete from dummy where pk = 0", this.onSuccess(r -> {
            // But insert it afterwards
            cassandra.execute("update dummy set val = ? where pk = ?", Arrays.asList(7, 0), this.onSuccess(r2 -> {
                cassandra.execute("select * from dummy", this.onSuccess(r3 -> {
                    this.assertEquals(1, r3.size());
                    this.assertArrayEquals(new Object[] { 0, 7 }, r3.getValues().get(0).toArray());
                    this.testComplete();
                }));
            }));
        }));
        this.await();
    }

    @Test
    public void futureTimestamp() throws InterruptedException {

        this.insertSomeValues(1);

        int millisToAdd = 1000;
        long fiveSecondsLater = Instant.now().plus(millisToAdd, ChronoUnit.MILLIS).toEpochMilli() * millisToAdd;
        ExecutionOptions options = new ExecutionOptions().setTimestamp(fiveSecondsLater);

        // See
        // http://www.planetcassandra.org/blog/an-introduction-to-using-custom-timestamps-in-cql3/
        // for details

        // Delete the row in the future
        cassandra.executeWithOptions("delete from dummy where pk = 0", options, this.onSuccess(r -> {
            // Update the row now
            cassandra.execute("update dummy set val = ? where pk = ?", Arrays.asList(7, 0), this.onSuccess(r2 -> {
                // Wait for the deletion to occur
                vertx.setTimer(millisToAdd * 2, l -> {
                    // Check it does not exist anymore
                    cassandra.execute("select * from dummy", this.onSuccess(r3 -> {
                        this.assertEquals(0, r3.size());
                        this.testComplete();
                    }));
                });
            }));
        }));
        this.await();
    }

    // ExecutionOptionsTest
    // - idempotent
    // - retryPolicy
    // - serialConsistencyLevel

}
