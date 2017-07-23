package pku.netlab.hermes.broker;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import pku.netlab.hermes.ClusterCommunicator;
import pku.netlab.hermes.broker.Impl.KafkaMQ;
import pku.netlab.hermes.broker.Impl.PubSubBroker;
import pku.netlab.hermes.broker.Impl.RedisSessionStore;
import pku.netlab.hermes.pushservice.IPushService;
import pku.netlab.hermes.pushservice.PushServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hult on 2017/7/22.
 */
public class ProcessorContext {
    private final Logger logger = LoggerFactory.getLogger(ProcessorContext.class);
    JsonObject config;
    Vertx vertx = Vertx.vertx();

    public ProcessorContext(JsonObject cfg) {
        this.config = cfg;
        String ip = Utils.getIpAddress(config.getJsonObject("broker").getString("network_id"));
        this.config.put("ip", ip);
        String brokerID = config.getJsonObject("broker").getString("broker_prefix") + "_" + ip;
        this.config.put("broker_id", brokerID);
        //TODO: async!!!
    }

    public void run() {
        CoreProcessor processor = new PubSubBroker();
        processor.setIpAddress(config.getString("ip"));
        processor.setBrokerID(config.getString("broker_id"));
        processor.setMessageQueue(createMessageQueue(processor));
        ISessionStore sessionStore = createSessionStore();
        processor.setSessionStore(sessionStore);
        processor.setMessageStore((IMessageStore)sessionStore);
        processor.setMqttServers(createMQTTServers(processor));
        processor.setLocalSessionMap(vertx.sharedData().getLocalMap("LOCAL_SESSION_MAP"));
        processor.setServerVertx(vertx);
        deployClusterCommunicator(processor);
        deployPushServer(processor);
    }

    private void deployPushServer(IPushService pushService) {
        PushServer pushServer = new PushServer(pushService);
        vertx.deployVerticle(pushServer, new DeploymentOptions().setConfig(config.getJsonObject("pushServer")));
    }

    private ISessionStore createSessionStore() {
        Vertx redisVertx = Vertx.vertx();
        return new RedisSessionStore(redisVertx, config.getJsonObject("redis"));
    }

    private IMessageQueue createMessageQueue(CoreProcessor processor) {
        //create this vertx so that it can be isolated from clustered-eventbus
        Vertx kafkaVertx = Vertx.vertx();
        JsonObject kafkaConfig = config.getJsonObject("kafka");
        kafkaConfig.put("broker_id", config.getString("broker_id"));
        return new KafkaMQ(kafkaVertx, kafkaConfig, processor::handleMsgFromMQ);
    }

    private List<MQTTServer> createMQTTServers(CoreProcessor processor) {
        List<MQTTServer> servers = new ArrayList<>();
        for (int i = 0 ; i < Runtime.getRuntime().availableProcessors(); i += 1) {
            MQTTServer server = new MQTTServer(processor);
            servers.add(server);
            vertx.deployVerticle(server,
                new DeploymentOptions().setConfig(config.getJsonObject("broker").put("ip", processor.getIpAddress())));
        }
        return servers;
    }

    /*
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
    */

    private void deployClusterCommunicator(CoreProcessor processor) {
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
        VertxOptions options = new VertxOptions().setClusterManager(manager).setClusterHost(config.getString("ip"));
        Vertx.clusteredVertx(options, res-> {
            if (res.succeeded()) {
                Vertx clusterVertx = res.result();
                ClusterCommunicator clusterCommunicator = new ClusterCommunicator(manager, processor);
                clusterVertx.deployVerticle(clusterCommunicator, dep-> {
                    if (dep.failed()) {
                        logger.error("fail to cluster communicator: " + dep.cause().getMessage());
                    } else {
                        processor.setClusterCommunicator(clusterCommunicator);
                        logger.info("cluster communicator deployed with id: " + manager.getNodeID());
                    }
                });
            } else {
                logger.error("fail to cluster brokers: " + res.cause().getMessage());
                System.exit(0);
            }
        });
    }



}
