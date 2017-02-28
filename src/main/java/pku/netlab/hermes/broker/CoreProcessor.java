package pku.netlab.hermes.broker;

import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import pku.netlab.hermes.ClusterCommunicator;
import pku.netlab.hermes.broker.Impl.KafkaMQ;
import pku.netlab.hermes.broker.Impl.RedisSessionStore;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by hult on 2/10/17.
 */
public class CoreProcessor {
    private final String brokerID;
    private final Vertx brokerVertx = Vertx.vertx();
    private final Logger logger = LoggerFactory.getLogger(CoreProcessor.class);
    private final JsonObject config;
    private ISessionStore sessionStore;
    private IMessageQueue messageQueue;
    private ConcurrentMap<String, MQTTSession> sessionLocalMap;
    private ArrayList<MQTTBroker> brokerList;

    public CoreProcessor(JsonObject config) {
        this.config = config;
        this.brokerID = config.getJsonObject("broker").getString("broker_id");
        this.brokerList = new ArrayList<>(Runtime.getRuntime().availableProcessors());
    }

    public void deployManyVerticles() {
        messageQueue = deployKafka();
        sessionStore = deploySessionStore();
        sessionLocalMap = new ConcurrentHashMap<>();
        deployStateServer();
        deployBrokers();
        deployClusterCommunicator();
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


    private void deployBrokers() {
        for (int i = 0 ; i < Runtime.getRuntime().availableProcessors(); i += 1) {
            MQTTBroker broker = new MQTTBroker(this);
            brokerList.add(broker);
            brokerVertx.deployVerticle(broker, new DeploymentOptions().setConfig(config.getJsonObject("broker")));
        }
    }

    private void deployStateServer() {
        HttpServer server = Vertx.vertx().createHttpServer();
        server.requestHandler(req-> {
            if (req.method() == HttpMethod.GET) {
                this.sessionStore.getAllMembers(brokerID, res-> {
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

    private void deployClusterCommunicator() {
        JsonObject zkConfig = config.getJsonObject("zookeepers");
        /**
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getString("zookeeperHosts"))
                .namespace(zkConfig.getString("rootPath", "io.vertx"))
                .sessionTimeoutMs(zkConfig.getInteger("sessionTimeout", 20000))
                .connectionTimeoutMs(zkConfig.getInteger("connectTimeout", 3000))
                .retryPolicy(new ExponentialBackoffRetry(
                        zkConfig.getJsonObject("retry", new JsonObject()).getInteger("initialSleepTime", 1000),
                        zkConfig.getJsonObject("retry", new JsonObject()).getInteger("maxTimes", 5),
                        zkConfig.getJsonObject("retry", new JsonObject()).getInteger("intervalTimes", 10000))
                ).build();
        curator.start();
        ZookeeperClusterManager manager = new ZookeeperClusterManager(curator, brokerID);
        */

        ZookeeperClusterManager manager = new ZookeeperClusterManager(zkConfig);
        VertxOptions options = new VertxOptions().setClusterManager(manager);

        Vertx.clusteredVertx(options, res-> {
            if (res.succeeded()) {
                Vertx clusterVertx = res.result();
                ClusterCommunicator clusterCommunicator = new ClusterCommunicator(manager, this);
                clusterVertx.deployVerticle(clusterCommunicator, dep-> {
                    if (dep.failed()) {
                        logger.error("fail to cluster communicator: " + dep.cause().getMessage());
                    } else {
                        logger.info("cluster communicator deployed with id: " + manager.getNodeID());
                    }
                });
            } else {
                logger.error("fail to cluster brokers: " + res.cause().getMessage());
                System.exit(0);
            }
        });
    }


    void clientLogin(String clientID, MQTTSession session, Handler<AsyncResult<Void>> handler){
        //keep track of which
        sessionLocalMap.put(clientID, session);
        sessionStore.addClient(brokerID, clientID, handler);
    };

    void clientLogout(String clientID){
        sessionLocalMap.remove(clientID);
        sessionStore.removeClient(brokerID, clientID, aVoid->{});
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
