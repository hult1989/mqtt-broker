package pku.netlab.hermes.broker;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.DisconnectMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessageWithKey;
import pku.netlab.hermes.ClusterCommunicator;
import pku.netlab.hermes.broker.Impl.KafkaMQ;
import pku.netlab.hermes.broker.Impl.RedisSessionStore;
import pku.netlab.hermes.parser.MQTTEncoder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hult on 2/10/17.
 * All methods of this class should be thread safe, since they will be called from different threads.
 */
public class CoreProcessor {

    private final String brokerID;
    private final Vertx brokerVertx;
    private final Logger logger = LoggerFactory.getLogger(CoreProcessor.class);
    private final JsonObject config;
    private final EventBus localEB;
    private ISessionStore sessionStore;
    private IMessageQueue messageQueue;
    private LocalMap<String, String> sessionLocalMap;
    private ArrayList<MQTTBroker> brokerList;
    private MQTTEncoder encoder;

    public CoreProcessor(JsonObject config) {
        this.brokerVertx = Vertx.vertx();
        this.config = config;
        this.brokerID = config.getJsonObject("broker").getString("broker_id");
        this.brokerList = new ArrayList<>(Runtime.getRuntime().availableProcessors());
        this.localEB = brokerVertx.eventBus();
        this.encoder = new MQTTEncoder();
    }

    public void deployManyVerticles() {
        messageQueue = deployKafka();
        sessionStore = deploySessionStore();
        sessionLocalMap = brokerVertx.sharedData().getLocalMap("SESSION_LOCAL_MAP");
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


    public void clientLogin(String clientID, Handler<AsyncResult<Void>> handler){
        //keep track of which
        //localEB.send(clientID, new DisconnectMessage());
        sessionStore.brokerOfClient(clientID, get-> {
            if (brokerID.equals(get.result())) {
                if (sessionLocalMap.get(clientID) != null) {
                    logger.info(clientID + " login from a new TCP connection");
                    try {
                        localEB.publish(clientID, encoder.enc(new DisconnectMessage()));
                        } catch(Exception e){
                            e.printStackTrace();
                        }
                } else {
                    sessionLocalMap.put(clientID, brokerID);
                }
                sessionStore.addClient(brokerID, clientID, handler);
            } else {
                //need broadcast clientID among cluster, let disconnect this Client from another broker
                sessionStore.addClient(brokerID, clientID, handler);
            }
        });
    }

    public void clientLogout(String clientID){
        //sessionLocalMap.remove(clientID);
        sessionStore.removeClient(brokerID, clientID, aVoid->{});
    };

    public void storeMessage(){};

    public void delMessage(String key, String clientID) {
        sessionStore.removePendingMessage(key, clientID);
    }

    public void delMessage(String key){
        sessionStore.removeMessage(key);
    };

    public String getClientSession(String clientID){
        return sessionLocalMap.get(clientID);
    }

    void updateClientStatus(){}

    public void handleMsgFromMQ(JsonObject msg){
        JsonObject value = new JsonObject(msg.getString("value"));
        try {
            JsonArray targets = value.getJsonArray("targets");
            PublishMessage publishMessage = new PublishMessage();
            publishMessage.setPayload(value.getString("msg"));
            publishMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);
            publishMessage.setMessageID((int)System.currentTimeMillis() % 65536);
            for (Object o: targets.getList()) {
                String client = (String) o;
                publishMessage.setTopicName(client);
                localEB.send(client, encoder.enc(publishMessage));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void saveSubscription(){};

    public void delSubscription(){};

    public void enqueKafka(PublishMessage publishMessage) {
        messageQueue.enQueue(publishMessage);
    }

    public void getPendingMessages(String clientID, Handler<List<PublishMessageWithKey>> handler) {
        sessionStore.pendingMessages(clientID, handler);
    }

}
