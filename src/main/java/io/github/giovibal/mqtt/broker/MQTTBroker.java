package io.github.giovibal.mqtt.broker;

import as.leap.vertx.rpc.impl.RPCClientOptions;
import as.leap.vertx.rpc.impl.VertxRPCClient;
import com.cyngn.kafka.consume.SimpleConsumer;
import io.github.giovibal.mqtt.ConfigParser;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by giovanni on 11/04/2014.
 * The Main Verticle
 */
public class MQTTBroker extends AbstractVerticle {

    private Logger logger = LoggerFactory.getLogger(MQTTBroker.class);
    public static String clusterID = null;
    public static String brokerID = null;
    public static ConcurrentHashMap<String, MQTTSession> onlineSessions = new ConcurrentHashMap<>();
    private static ISessionStore sessionStore;

    public static ISessionStore getSessionStore () {
        return sessionStore;
    }

    private void deployVerticle(String c, DeploymentOptions opt) {
        vertx.deployVerticle(c, opt,
                result -> {
                    if (result.failed()) {
                        result.cause().printStackTrace();
                    } else {
                        String deploymentID = result.result();
                        logger.debug(c + ": " + deploymentID);
                    }
                }
        );
    }
    private void deployVerticle(Class c, DeploymentOptions opt) {
        vertx.deployVerticle(c.getName(), opt,
                result -> {
                    if (result.failed()) {
                        result.cause().printStackTrace();
                    } else {
                        String deploymentID = result.result();
                        logger.debug(c.getSimpleName() + ": " + deploymentID);
                    }
                }
        );
    }

    private void deployStoreVerticle() {
        RPCClientOptions<ISessionStore> rpcClientOptions = new RPCClientOptions<ISessionStore>(vertx)
                .setBusAddress("REDIS_SESSION_STORE").setServiceClass(ISessionStore.class);
        sessionStore = new VertxRPCClient<ISessionStore>(rpcClientOptions).bindService();
        /*
        deployVerticle(StoreVerticle.class,
                new DeploymentOptions().setWorker(false).setInstances(instances)
        );
        */
    }

    private void deployKafka(JsonObject config) {
        JsonObject kafkaConfig = config.getJsonObject("kafka");
        kafkaConfig.put("topics", new JsonArray().add(brokerID));
        String kafkaAddress = String.join("/", SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, brokerID);
        kafkaConfig.put("eventbus.address", kafkaAddress);


        vertx.eventBus().consumer(kafkaAddress, msg-> {
            System.out.println("From Kafka: " + msg.body().toString());
        });

        vertx.deployVerticle(SimpleConsumer.class.getName(), new DeploymentOptions().setConfig(kafkaConfig), kret-> {
            if (!kret.succeeded()) {
                System.out.println(kret.cause().getMessage());
                System.exit(0);
            } else {
                logger.info("kafka deployment finished, with topic: " + kafkaConfig.getJsonArray("topics"));
            }
        });
    }


    @Override
    public void start() {
        try {
            JsonObject config = config();
            clusterID = config.getString("cluster_id");
            brokerID = config.getJsonObject("broker").getString("broker_id");


            // 1 store x 1 broker
            deployStoreVerticle();
            deployStateServer();


            JsonObject brokerConf = config.getJsonObject("broker");
            ConfigParser c = new ConfigParser(brokerConf);
                // MQTT over TCP
            startTcpServer(c);
            deployKafka(config);

            logger.info("Startd Broker ==> [port: " + c.getPort() + "]" +
                            " [" + c.getFeatursInfo() + "] " +
                            " [socket_idle_timeout:" + c.getSocketIdleTimeout() + "] ");
            logger.info("cluster broker deployed with id: " + clusterID);


        } catch(Exception e ) {
            logger.error(e.getMessage(), e);
        }
    }

    private void deployStateServer() {
        HttpServer server = vertx.createHttpServer();
        server.requestHandler(req-> {
            if (req.method() == HttpMethod.GET) {
                req.response().end(sessionStore.toString());
            }
        });
        server.listen(8989);
    }


    private void startTcpServer(ConfigParser c) {
        int port = c.getPort();
        int idleTimeout = c.getSocketIdleTimeout();

        NetServerOptions opt = new NetServerOptions()
                .setTcpKeepAlive(true)
                .setAcceptBacklog(16384)
                .setIdleTimeout(idleTimeout) // in seconds; 0 means "don't timeout".
                .setReceiveBufferSize(4096)
                .setSendBufferSize(4096)
                .setPort(port);

        NetServer netServer = vertx.createNetServer(opt);
        netServer.connectHandler(netSocket -> {
            MQTTSocket mqttSocket = new MQTTSocket(vertx, c, netSocket);
            logger.info("a client connected from " + netSocket.remoteAddress());
            //TODO: make sessionStore and onlineUsers thread-safe
            mqttSocket.start();
        }).listen();
    }
}
