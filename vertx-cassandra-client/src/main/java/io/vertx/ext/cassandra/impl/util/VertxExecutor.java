package io.vertx.ext.cassandra.impl.util;

import io.vertx.core.Vertx;

import java.util.concurrent.Executor;

public class VertxExecutor implements Executor {

    private Vertx vertx;

    public VertxExecutor(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void execute(Runnable command) {
        vertx.runOnContext((v) -> command.run());
    }
}
