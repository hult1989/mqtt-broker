package io.github.giovibal.mqtt.broker.Impl;

import io.github.giovibal.mqtt.broker.ISessionStore;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;

/**
 * Created by hult on 1/13/17.
 */
public class RedisSessionStore implements ISessionStore{
    Logger logger = LoggerFactory.getLogger(RedisSessionStore.class);
    private RedisClient redisClient;

    public RedisSessionStore(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public void addClient(String brokerID, String clientID, Handler<AsyncResult<Void>> handler) {
        Future<Long> f1 = Future.future();
        Future<Void> f2 = Future.future();
        this.redisClient.sadd(brokerID, clientID, f1.completer());
        this.redisClient.set(clientID, brokerID, f2.completer());
        CompositeFuture.all(f1, f2).setHandler(ar-> {
            handler.handle(ar.succeeded()? Future.succeededFuture(): Future.failedFuture(ar.cause()));
        });
    }

    @Override
    public void removeClient(String brokerID, String clientID, Handler<AsyncResult<Void>> handler) {
        Future<Long> f1 = Future.future();
        Future<Long> f2 = Future.future();
        this.redisClient.srem(brokerID, clientID, f1.completer());
        this.redisClient.del(clientID, f2.completer());
        CompositeFuture.all(f1, f2).setHandler(ar-> {
            handler.handle(ar.succeeded() ? Future.succeededFuture() : Future.failedFuture(ar.cause()));
        });
    }

    @Override
    public void brokerOfClient(String clientID, Handler<AsyncResult<String>> handler) {
        this.redisClient.get(clientID, get-> {
            handler.handle(get);
        });
    }

    @Override
    public void clearBrokerSession(String brokerID, Handler<AsyncResult<Void>> handler) {
        this.redisClient.smembers(brokerID, mem-> {
            if (!mem.succeeded()) {
                handler.handle(Future.failedFuture(mem.cause()));
            } else {
                Future<Long> delKVFuture = Future.future();
                Future<Long> delSetFuture = Future.future();
                this.redisClient.delMany(mem.result().getList(), delKVFuture.completer());
                this.redisClient.del(brokerID, delSetFuture.completer());
                CompositeFuture.all(delKVFuture, delSetFuture).setHandler(ar-> {
                    handler.handle(ar.succeeded() ? Future.succeededFuture() : Future.failedFuture(ar.cause()));
                });
            }
        });
    }
}

