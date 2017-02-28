package pku.netlab.hermes.broker;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

import java.util.HashMap;


public class MQTTBroker extends AbstractVerticle {
    public final CoreProcessor processor;
    private HashMap<String, MQTTSession> threadLocalSessionMap;
    private Logger logger = LoggerFactory.getLogger(MQTTBroker.class);
    private Context threadLocalContext;

    public MQTTBroker(CoreProcessor processor) {
        this.processor = processor;
        this.threadLocalSessionMap = new HashMap<>();
    }

    @Override
    public void start() {
        try {
            this.threadLocalContext = vertx.getOrCreateContext();
            JsonObject brokerConf = config();
            startTcpServer(brokerConf);
            logger.info(String.format("Start Broker ==> [port: %d] [socket_idle_timeout: %d] [%s]",
                    brokerConf.getInteger("tcp_port"), brokerConf.getInteger("socket_idle_timeout"), Thread.currentThread().getName()));

        } catch(Exception e ) {
            logger.error(e.getMessage(), e);
        }
    }

    //deploy debug server

    private void startTcpServer(JsonObject c) {
        int port = c.getInteger("tcp_port");
        int idleTimeout = c.getInteger("socket_idle_timeout");

        NetServerOptions opt = new NetServerOptions()
                .setTcpKeepAlive(true)
                .setAcceptBacklog(16384)
                .setIdleTimeout(idleTimeout) // in seconds; 0 means "don't timeout".
                .setReceiveBufferSize(4096)
                .setSendBufferSize(4096)
                .setPort(port);

        NetServer netServer = vertx.createNetServer(opt);
        netServer.connectHandler(netSocket -> {
            MQTTSocket mqttSocket = new MQTTSocket(vertx, netSocket, this);
            logger.info("a client connected from " + netSocket.remoteAddress());
            //TODO: make sessionStore and onlineUsers thread-safe
            mqttSocket.start();
        }).listen();
    }

    public void shutDownClientSession(String clientID) {
        threadLocalContext.runOnContext(aVoid-> {
            MQTTSession session = threadLocalSessionMap.get(clientID);
            if (session != null) {
                session.shutdown();
                threadLocalSessionMap.remove(clientID);
            }
        });
    }

    public void addSession(String clientID, MQTTSession session) {
        threadLocalContext.runOnContext(aVoid-> {
            threadLocalSessionMap.put(clientID, session);
        });
    }

    public void sendMessage(String clientID, PublishMessage msg) {
        threadLocalContext.runOnContext(aVoid-> {
            MQTTSession session = threadLocalSessionMap.get(clientID);
            if (session != null) {
                session.sendPublishMessage(msg);
            }
        });
    }
}
