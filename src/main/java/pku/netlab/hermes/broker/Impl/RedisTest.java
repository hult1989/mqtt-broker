package pku.netlab.hermes.broker.Impl;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

/**
 * Created by hult on 1/13/17.
 */
public class RedisTest extends AbstractVerticle {
    @Override
    public void start() throws Exception {
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(RedisTest.class.getName());
    }
}
