package pku.netlab.hermes.broker;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.dna.mqtt.moquette.proto.messages.DisconnectMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessageWithKey;
import pku.netlab.hermes.ClusterCommunicator;
import pku.netlab.hermes.broker.Impl.KafkaMQ;
import pku.netlab.hermes.broker.Impl.RedisSessionStore;
import pku.netlab.hermes.parser.MQTTEncoder;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by hult on 2/10/17.
 * All methods of this class should be thread safe, since they will be called from different threads.
 */
public class CoreProcessor {

    private final Vertx brokerVertx;
    private final Logger logger = LoggerFactory.getLogger(CoreProcessor.class);
    private final JsonObject config;
    private final EventBus brokerEB;
    private ClusterCommunicator clusterCommunicator;
    private ISessionStore sessionStore;
    private IMessageQueue messageQueue;
    private LocalMap<String, String> sessionLocalMap;
    private List<MQTTBroker> mqttBrokers;
    private MQTTEncoder encoder;
    private String brokerID;
    private String ipAddress;

    private String getIpAddress(String networkID) {
        Stream<String> ipStream = Stream.empty();
        try {
            Enumeration<NetworkInterface > enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface ni = enumeration.nextElement();
                ipStream = Stream.concat(ipStream, ni.getInterfaceAddresses().stream().map(ia -> ia.getAddress().getHostAddress()));
            }
        } catch (SocketException e) {
            logger.error(e.getMessage());
            System.exit(0);
        }
        String ret = ipStream.filter(ip -> ip.startsWith(networkID)).findFirst().get();
        if (ret == null) {
            logger.error("failed to find ip address starts with " + networkID);
            System.exit(0);
        }
        return ret;
    }

    public CoreProcessor(JsonObject config) {
        //this vertx is created to init all BrokerVerticle
        this.brokerVertx = Vertx.vertx();
        this.config = config;
        this.ipAddress = getIpAddress(config.getJsonObject("broker").getString("network_id"));
        this.brokerID = config.getJsonObject("broker").getString("broker_prefix") + "_" + this.ipAddress;
        this.mqttBrokers = new ArrayList<>(Runtime.getRuntime().availableProcessors());
        this.brokerEB = brokerVertx.eventBus();
        this.encoder = new MQTTEncoder();
        deployClusterCommunicator();
    }

    public void start() {
        if (this.brokerID.equals("standby")) {
            logger.info("This is a standby broker, waiting...");
        } else {
            deployManyVerticles();
        }
    }

    private void deployManyVerticles() {
        messageQueue = deployKafka();
        sessionStore = deploySessionStore();
        sessionLocalMap = brokerVertx.sharedData().getLocalMap("SESSION_LOCAL_MAP");
        deployStateServer();
        deployBrokers();
    }

    public String getBrokerID() {
        return this.brokerID;
    }

    public void updateBrokerID(String id) {
        this.brokerID = id;
    }

    public String getHost() {
        return this.ipAddress;
    }

    private ISessionStore deploySessionStore() {
        Vertx redisVertx = Vertx.vertx();
        return new RedisSessionStore(redisVertx, config.getJsonObject("redis"));
    }

    private IMessageQueue deployKafka() {
        //create this vertx so that it can be isolated from clustered-eventbus
        Vertx kafkaVertx = Vertx.vertx();
        JsonObject kafkaConfig = config.getJsonObject("kafka");
        kafkaConfig.put("brokerID", this.brokerID);
        return new KafkaMQ(kafkaVertx, kafkaConfig, this::handleMsgFromMQ);
    }


    private void deployBrokers() {
        for (int i = 0 ; i < Runtime.getRuntime().availableProcessors(); i += 1) {
            MQTTBroker broker = new MQTTBroker(this);
            mqttBrokers.add(broker);
            brokerVertx.deployVerticle(broker,
                new DeploymentOptions().setConfig(config.getJsonObject("broker").put("ip", this.ipAddress)));
        }
    }

    private void deployStateServer() {
        HttpServer server = Vertx.vertx().createHttpServer(new HttpServerOptions()
                .setPort(8989).setTcpNoDelay(true).setTcpNoDelay(true).setHost(this.ipAddress));
        logger.info("state server starts at: " + this.ipAddress);
        server.requestHandler(req-> {
            if (req.method() == HttpMethod.GET) {
                if (req.path().contains("users")) {
                    this.sessionStore.getAllMembers(brokerID, res -> {
                        if (!res.succeeded()) {
                            logger.warn("failed to get all members from redis");
                        }
                        String ret = String.format("Redis: {%s}\nLocalMap: {%s}\n",
                                res.result().toString(), sessionLocalMap);
                        req.response().end(ret);
                    });
                } else if (req.path().contains("brokers")) {
                    req.response().end(this.clusterCommunicator.getBrokers());
                }
            }
        });
        server.listen();
    }

    private void deployClusterCommunicator() {
        JsonObject zkConfig = config.getJsonObject("zookeepers");
        /*
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
        VertxOptions options = new VertxOptions().setClusterManager(manager).setClusterHost(this.ipAddress);

        Vertx.clusteredVertx(options, res-> {
            if (res.succeeded()) {
                Vertx clusterVertx = res.result();
                this.clusterCommunicator = new ClusterCommunicator(manager, this);
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
        //brokerEB.send(clientID, new DisconnectMessage());
        sessionStore.brokerOfClient(clientID, get-> {
            if (brokerID.equals(get.result())) {
                if (sessionLocalMap.get(clientID) != null) {
                    logger.info(clientID + " login from a new TCP connection");
                    try {
                        brokerEB.publish(clientID, encoder.enc(new DisconnectMessage()));
                        } catch(Exception e){
                            e.printStackTrace();
                        }
                } else {
                    sessionLocalMap.put(clientID, brokerID);
                }

                sessionStore.addClient(brokerID, clientID, handler);
            } else {
                //need broadcast clientID among cluster, let disconnect this Client from another broker
                sessionLocalMap.put(clientID, brokerID);
                sessionStore.addClient(brokerID, clientID, handler);
            }
        });
    }

    public void clientLogout(String clientID){
        sessionLocalMap.remove(clientID);
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

    public void handleMsgFromMQ(byte[] byteMsg){
        /*
        JsonObject value = new JsonObject(msg.getString("value"));
        try {
            PendingMessage pending = new PendingMessage(value);
            logger.info("pending message: " + pending);
            PublishMessage publish = new PublishMessage();
            publish.setPayload(pending.msg);
            publish.setQos(AbstractMessage.QOSType.LEAST_ONE);
            //a random message id is OK since it will be rewritten in MQTTSession
            publish.setMessageID((int)System.currentTimeMillis() % 65536);
            for (Object o: pending.targets) {
                String client = (String) o;
                if (sessionLocalMap.get(client) == null) {
                    logger.error("cannot find " + client + " in sessionLocalMap");
                }
                publish.setTopicName(client);
                brokerEB.send(client, encoder.enc(publish));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        */
    }

    public void saveSubscription(){};

    public void delSubscription(){};

    public void enqueKafka(PublishMessage publishMessage) {
        messageQueue.enQueue(publishMessage);
    }

    public void getPendingMessages(String clientID, Handler<List<PublishMessageWithKey>> handler) {
        sessionStore.pendingMessages(clientID, handler);
    }

    public EventBus getBrokerEB() {
        return brokerEB;
    }
}
