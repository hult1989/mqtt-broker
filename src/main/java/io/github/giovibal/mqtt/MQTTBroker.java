package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.persistence.StoreVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.PemKeyCertOptions;

/**
 * Created by giovanni on 11/04/2014.
 * The Main Verticle
 */
public class MQTTBroker extends AbstractVerticle {

    private Logger logger = LoggerFactory.getLogger(MQTTBroker.class);

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

    private void deployStoreVerticle(int instances) {
        deployVerticle(StoreVerticle.class,
                new DeploymentOptions().setWorker(false).setInstances(instances)
        );
    }


    @Override
    public void start() {
        try {
            JsonObject config = config();

            // 1 store x 1 broker
            deployStoreVerticle(1);

            JsonArray brokers = config.getJsonArray("brokers");
            for(int i = 0; i < brokers.size(); i++) {
                JsonObject brokerConf = brokers.getJsonObject(i);
                ConfigParser c = new ConfigParser(brokerConf);
                // MQTT over TCP
                startTcpServer(c);
                logger.info(
                        "Startd Broker ==> [port: " + c.getPort() + "]" +
                                " [" + c.getFeatursInfo() + "] " +
                                " [socket_idle_timeout:" + c.getSocketIdleTimeout() + "] "
                );
            }
        } catch(Exception e ) {
            logger.error(e.getMessage(), e);
        }

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
            MQTTNetSocket mqttNetSocket = new MQTTNetSocket(vertx, c, netSocket);
            logger.info("a client connected from " + netSocket.remoteAddress());
            mqttNetSocket.start();
        }).listen();
    }
}
