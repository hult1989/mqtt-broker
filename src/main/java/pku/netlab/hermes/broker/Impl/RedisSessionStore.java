package pku.netlab.hermes.broker.Impl;

import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.dna.mqtt.moquette.proto.messages.PublishMessageWithKey;
import pku.netlab.hermes.broker.ISessionStore;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hult on 1/13/17.
 * use redis set and string two data structure
 * set: broker-001-> {hult, alice, bob...}
 * string: hult: broker-001
 */
public class RedisSessionStore implements ISessionStore{
    Logger logger = LoggerFactory.getLogger(RedisSessionStore.class);
    private RedisClient redisClient;

    public RedisSessionStore(Vertx vertx, JsonObject config) {
        this.redisClient = RedisClient.create(vertx, new RedisOptions(config));
        logger.info("redis deployed at " + Thread.currentThread().getName());
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

    @Override
    public void getAllMembers(String brokerID, Handler<AsyncResult<JsonArray>> handler) {
        this.redisClient.smembers(brokerID, handler);
    }

    @Override
    public void pendingMessages(String clientID, Handler<List<PublishMessageWithKey>> handler) {
        String pendingListName = setName(clientID);
        this.redisClient.smembers(pendingListName, smem-> {
            if (smem.succeeded()) {
                List<String> keys = smem.result().getList();
                this.redisClient.mgetMany(keys, mget-> {
                    if (mget.succeeded()) {
                        List<String> messages = mget.result().getList();
                        List<PublishMessageWithKey> pubList = new ArrayList<>();
                        List<String> toRem = new ArrayList<>();
                        for (int i = 0; i < messages.size(); i += 1) {
                            String strPbu = messages.get(i);
                            String key = keys.get(i);
                            try {
                                PublishMessageWithKey pub = new PublishMessageWithKey(strPbu, key);
                                pubList.add(pub);
                            } catch (Exception e) {
                                toRem.add(key);
                                logger.warn("fail to decode: " + strPbu);
                            }
                        }
                        handler.handle(pubList);
                        this.redisClient.sremMany(pendingListName, toRem, aVoid->{});
                        this.redisClient.delMany(toRem, aVoid-> {});
                    }
                });
            }
        });
    }

    @Override
    public void removePendingMessage(String key, String clientID) {
        this.removeMessage(key);
        this.redisClient.srem(setName(clientID), key, aVoid->{});
    }

    @Override
    public void removeMessage(String key) {
        this.redisClient.del(key, aVoid -> {});
    }

    private String setName(String clientID) {
        return "pending:" + clientID;
    }
}

