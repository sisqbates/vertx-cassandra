package io.vertx.ext.cassandra.impl.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import com.google.common.util.concurrent.FutureCallback;

public class DelegateFutureCallback<V> implements FutureCallback<V> {

    private Handler<AsyncResult<V>> delegate;

    public DelegateFutureCallback(Handler<AsyncResult<V>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onFailure(Throwable t) {
        delegate.handle(Future.failedFuture(t));
    }

    @Override
    public void onSuccess(V result) {
        delegate.handle(Future.succeededFuture(result));
    }

}
