package pku.netlab.hermes.broker;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import pku.netlab.hermes.ConfigParser;
import pku.netlab.hermes.broker.Impl.KafkaMQ;
import pku.netlab.hermes.broker.Impl.RedisSessionStore;

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
    private static IMessageQueue messageQueue;
    private static String REDIS_EVENTBUS_ADDR = "REDIS_SESSION_STORE";

    public static ISessionStore getSessionStore () {
        if (sessionStore == null) {
            System.err.println("Session Store not init, exit!");
            System.exit(0);
        }
        return sessionStore;
    }

    @Override
    public void start() {
        try {
            JsonObject config = config();
            this.clusterID = config.getString("cluster_id");
            this.brokerID = config.getJsonObject("broker").getString("broker_id");

            deployStoreVerticle(config);


            JsonObject brokerConf = config.getJsonObject("broker");
            ConfigParser c = new ConfigParser(brokerConf);
                // MQTT over TCP
            startTcpServer(c);
            deployMessageQueue(config);

            logger.info("Startd Broker ==> [port: " + c.getPort() + "]" +
                            " [" + c.getFeatursInfo() + "] " +
                            " [socket_idle_timeout:" + c.getSocketIdleTimeout() + "] ");
            logger.info("cluster broker deployed with id: " + clusterID);

            deployStateServer();
        } catch(Exception e ) {
            logger.error(e.getMessage(), e);
        }
    }

    //deploy debug server
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

    //deploy session store
    private void deployStoreVerticle(JsonObject config) {
        /*
        RPCClientOptions<ISessionStore> rpcClientOptions = new RPCClientOptions<ISessionStore>(vertx)
                .setBusAddress(REDIS_EVENTBUS_ADDR).setServiceClass(ISessionStore.class);
        sessionStore = new VertxRPCClient<ISessionStore>(rpcClientOptions).bindService();
        */
        RedisClient client = RedisClient.create(vertx, new RedisOptions(config.getJsonObject("redis")));
        this.sessionStore = new RedisSessionStore(client);
    }

    private void deployMessageQueue(JsonObject config) {
        messageQueue = new KafkaMQ(vertx, config);
        messageQueue.setMessageHandler(msg-> {
        //TODO poll message from mq
            System.out.println("From Kafka: " + msg);
        });
        logger.info("kafka deployment finished");
   }
}
