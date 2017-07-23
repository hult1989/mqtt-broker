package pku.netlab.hermes.broker.Impl;

import com.sun.javafx.fxml.expression.KeyPath;
import com.sun.org.apache.xpath.internal.operations.Bool;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.dna.mqtt.moquette.proto.messages.PublishMessageWithKey;
import pku.netlab.hermes.broker.IMessageStore;
import pku.netlab.hermes.broker.ISessionStore;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by hult on 1/13/17.
 * use redis set and string two data structure
 * set: broker:broker-001 -> {hult, alice, bob...}
 * key-value: user:${user} -> broker-001
 * set: pending:${user} -> {}
 * key-value: msg:${id }-> message
 * TODO: key-value can be optimized by using HashSet in Redis
 *
 */

public class RedisSessionStore implements ISessionStore, IMessageStore{
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
        this.redisClient.get(clientID, handler);
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

    @Override
    public void batchGet(List<String> messageIDs, Handler<List<String>> handler) {
        this.redisClient.mgetMany(messageIDs, mget-> {
            if (mget.succeeded()) {
                handler.handle(mget.result().getList());
            } else {
                logger.error("failed to get {%s} from Redis", messageIDs);
            }
        });
    }

    @Override
    public void batchRem(List<String> messageIDs, Handler<Boolean> handler) {
        this.redisClient.delMany(messageIDs, del-> handler.handle(del.succeeded()));
    }

    @Override
    public void rem(String id, Handler<Boolean> handler) {
        this.redisClient.del(id, del -> handler.handle(del.succeeded()));

    }

    @Override
    public void get(String id, Handler<String> handler) {
        this.redisClient.get(id, get-> {
            if (get.succeeded()) {
                handler.handle(get.result());
            } else {
                handler.handle(null);
            }
        });
    }

    @Override
    public void save(String message, Handler<String> handler) {
        String key = UUID.randomUUID().toString();
        this.redisClient.set(key, message, set-> {
            if (set.succeeded()) {
                handler.handle(key);
            } else {
                logger.error("failed to save message: " + message);
            }
        });
    }

    @Override
    public void batchSave(List<String> messages, Handler<List<String>> handler) {
        JsonObject keyvals = new JsonObject();
        List<String> keyList = new ArrayList<>(messages.size());
        messages.forEach(msg-> {
            String randomKey = UUID.randomUUID().toString();
            keyvals.put(randomKey, msg);
        });
        this.redisClient.mset(keyvals, set-> {
            if (!set.succeeded()) {
                logger.error("failed to set");
            } else {
                handler.handle(keyList);
            }
        });
    }
}

