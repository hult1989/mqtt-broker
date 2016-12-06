package io.github.giovibal.mqtt.pushservice;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

/**
 * Created by kindt on 2016/7/23 0023.
 * 在redis中保留了以下数据结构
 * map：key为clientid:mseq，value是全局消息id，可以由此在MongoDB中检索到消息全文，如alex:1024
 * queue：redis中保留了所有在线用户的消息队列，队列名为mq:clientid，队列内容为mseq，队列名如mq:alex
 * hash: 用来保存用户的当前mseq，以及所在的机器ip，key为user:clientid，如user:alex，字段包括mseq，serveraddr，useraddr
 * erveraddr是用来判断客户究竟连在了那台机器上，useraddr是用来判断用户是否在线已经是否有重复连接的现象
 */
public class DBClient {
    private static RedisClient redisClient = null;
    private static MongoClient mongoClient = null;

    public static synchronized void init(Vertx vertx) {
        JsonObject config = new JsonObject().put("host", "localhost").put("port", 27017)
                .put("username", "didi").put("password", "f").put("authSource", "msg").put("db_name", "msg");
        mongoClient = MongoClient.createNonShared(vertx, config);
        redisClient = RedisClient.create(vertx, new RedisOptions().setHost("localhost").setAuth("didi"));
    }

    //TODO: 改成单例，类似 return this.getInstance().redisClient;
    public static RedisClient getRedisClient() {
        return redisClient;
    }


    public static MongoClient getMongoClient() {
        return mongoClient;
    }
}
