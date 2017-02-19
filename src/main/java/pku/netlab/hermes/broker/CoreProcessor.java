package pku.netlab.hermes.broker;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import pku.netlab.hermes.broker.Impl.KafkaMQ;
import pku.netlab.hermes.broker.Impl.RedisSessionStore;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by hult on 2/10/17.
 */
public class CoreProcessor {
    private static CoreProcessor INSTANCE;
    private Logger logger = LoggerFactory.getLogger(CoreProcessor.class);
    private JsonObject config;
    private ISessionStore sessionStore;
    private IMessageQueue messageQueue;
    private ConcurrentMap<String, MQTTSession> sessionLocalMap;

    public CoreProcessor(JsonObject config) {
        this.config = config;
        INSTANCE = this;
    }

    public void deployManyVerticles() {
        messageQueue = deployKafka();
        sessionStore = deploySessionStore();
        sessionLocalMap = new ConcurrentHashMap<>();
        deployStateServer();
    }

    private ISessionStore deploySessionStore() {
        Vertx redisVertx = Vertx.vertx();
        return new RedisSessionStore(redisVertx, config.getJsonObject("redis"));
    }

    private IMessageQueue deployKafka() {
        //create this vertx so that it can be isolated from clustered-eventbus
        Vertx kafkaVertx = Vertx.vertx();
        JsonObject kafkaConfig = config.getJsonObject("kafka");
        kafkaConfig.put("brokerID", config.getJsonObject("broker").getString("broker_id"));
        return new KafkaMQ(kafkaVertx, kafkaConfig, this::handleMsgFromMQ);
    }

    static CoreProcessor getInstance() {
        if (INSTANCE == null) {
            System.err.println("CoreProcessor should be instanced first");
            System.exit(0);
        }
        return INSTANCE;
    }

    private void deployStateServer() {
        HttpServer server = Vertx.vertx().createHttpServer();
        server.requestHandler(req-> {
            if (req.method() == HttpMethod.GET) {
                this.sessionStore.getAllMembers(MQTTBroker.brokerID, res-> {
                    if (!res.succeeded()) {
                        logger.warn("failed to get all members from redis");
                    }
                    String ret = String.format("Redis: {%s}\nLocalMap: {%s}\n",
                            res.result().toString(), sessionLocalMap.toString());
                    req.response().end(ret);
                });
            }
        });
        server.listen(8989);
    }



    void clientLogin(String clientID, MQTTSession session, Handler<AsyncResult<Void>> handler){
        sessionLocalMap.put(clientID, session);
        sessionStore.addClient(MQTTBroker.brokerID, clientID, handler);
    };

    void clientLogout(String clientID){
        sessionLocalMap.remove(clientID);
        sessionStore.removeClient(MQTTBroker.brokerID, clientID, aVoid->{});
    };

    void storeMessage(){};

    void delMessage(){};

    MQTTSession getClientSession(String clientID){
        return sessionLocalMap.get(clientID);
    }

    void updateClientStatus(){}

    void handleMsgFromMQ(JsonObject msg){
        System.out.println(Thread.currentThread().getName() + msg.toString());
    }

    void saveSubscription(){};

    void delSubscription(){};

    void enqueKafka(PublishMessage publishMessage) {
        messageQueue.enQueue(publishMessage);

    }

}
