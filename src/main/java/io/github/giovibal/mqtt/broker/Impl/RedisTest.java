package io.github.giovibal.mqtt.broker.Impl;

import as.leap.vertx.rpc.RPCServer;
import as.leap.vertx.rpc.impl.RPCServerOptions;
import as.leap.vertx.rpc.impl.VertxRPCServer;
import io.github.giovibal.mqtt.broker.ISessionStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

/**
 * Created by hult on 1/13/17.
 */
public class RedisTest extends AbstractVerticle {
    @Override
    public void start() throws Exception {
        RedisClient client = RedisClient.create(vertx, new RedisOptions().setHost("laptop"));
        ISessionStore sessionStore = new RedisSessionStore(client);
        RPCServerOptions options = new RPCServerOptions(vertx).setBusAddress("REDIS_SESSION_STORE")
                .addService(sessionStore);
        RPCServer server = new VertxRPCServer(options);
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(RedisTest.class.getName());
    }
}
