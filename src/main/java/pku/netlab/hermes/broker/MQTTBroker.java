package pku.netlab.hermes.broker;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import pku.netlab.hermes.ConfigParser;

import java.util.concurrent.ConcurrentHashMap;


public class MQTTBroker extends AbstractVerticle {

    public CoreProcessor processor;
    private Logger logger = LoggerFactory.getLogger(MQTTBroker.class);

    public static String clusterID = null;
    public static String brokerID = null;
    public static ConcurrentHashMap<String, MQTTSession> onlineSessions = new ConcurrentHashMap<>();

    private static ISessionStore sessionStore;
    private static IMessageQueue messageQueue;

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
            processor = CoreProcessor.getInstance();
            ConfigParser c = new ConfigParser(config());
            startTcpServer(c);
            logger.info(String.format("Start Broker ==> [port: %d] [%s] [socket_idle_timeout: %d] [%s]",
                    c.getPort(), c.getFeatursInfo(), c.getSocketIdleTimeout(), Thread.currentThread().getName()));

        } catch(Exception e ) {
            logger.error(e.getMessage(), e);
        }
    }

    //deploy debug server

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
            MQTTSocket mqttSocket = new MQTTSocket(vertx, netSocket, processor);
            logger.info("a client connected from " + netSocket.remoteAddress());
            //TODO: make sessionStore and onlineUsers thread-safe
            mqttSocket.start();
        }).listen();
    }

}
