package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.MQTTSession;
import io.github.giovibal.mqtt.security.CertInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.*;

/**
 * Created by Giovanni Bleani on 15/07/2015.
 */
public class EventBusBridgeClientVerticle extends AbstractVerticle implements Handler<AsyncResult<NetSocket>> {

    private static Logger logger = LoggerFactory.getLogger(EventBusBridgeClientVerticle.class);
    
    private NetClient netClient;
    private String remoteBridgeHost;
    private Integer remoteBridgePort;
    private String address;
    private long connectionTimerID;
    private boolean connected;
    private String tenant;

    @Override
    public void start() throws Exception {

        JsonObject conf = config();

        remoteBridgeHost = conf.getString("remote_bridge_host", "localhost");
        remoteBridgePort = conf.getInteger("remote_bridge_port", 7007);
        address = MQTTSession.ADDRESS;
        tenant = conf.getString("remote_bridge_tenant");
        int idelTimeout = conf.getInteger("socket_idle_timeout", 30);


        // [TCP <- BUS] listen BUS write to TCP
        int timeout = 1000;
        NetClientOptions opt = new NetClientOptions()
                .setConnectTimeout(timeout) // 60 seconds
                .setTcpKeepAlive(true)
                .setIdleTimeout(idelTimeout)
            ;

        String ssl_cert_key = conf.getString("ssl_cert_key");
        String ssl_cert = conf.getString("ssl_cert");
        String ssl_trust = conf.getString("ssl_trust");
        if(ssl_cert_key != null && ssl_cert != null && ssl_trust != null) {
            opt.setSsl(true)
                    .setPemKeyCertOptions(new PemKeyCertOptions()
                                    .setKeyPath(ssl_cert_key)
                                    .setCertPath(ssl_cert)
                    )
                    .setPemTrustOptions(new PemTrustOptions()
                                    .addCertPath(ssl_trust)
                    )
            ;
            tenant = new CertInfo(ssl_cert).getTenant();
        }

        netClient = vertx.createNetClient(opt);
        netClient.connect(remoteBridgePort, remoteBridgeHost, this);
        connectionTimerID = vertx.setPeriodic(timeout*2, aLong -> {
            checkConnection();
        });
    }

    private void checkConnection() {
        if(!connected) {
            logger.info("Bridge Client - try to reconnect to server [" + remoteBridgeHost + ":" + remoteBridgePort + "] ... " + connectionTimerID);
            netClient.connect(remoteBridgePort, remoteBridgeHost, this);
        }
    }

    @Override
    public void handle(AsyncResult<NetSocket> netSocketAsyncResult) {
        if (netSocketAsyncResult.succeeded()) {
            NetSocket netSocket = netSocketAsyncResult.result();
            final EventBusNetBridge ebnb = new EventBusNetBridge(netSocket, vertx.eventBus(), address);
            connected = true;
            logger.info("Bridge Client - connected to server [" + remoteBridgeHost + ":" + remoteBridgePort + "] " + netSocket.writeHandlerID());
            netSocket.closeHandler(aVoid -> {
                logger.info("Bridge Client - closed connection from server [" + remoteBridgeHost + ":" + remoteBridgePort + "] " + netSocket.writeHandlerID());
                ebnb.stop();
                connected = false;
            });
            netSocket.exceptionHandler(throwable -> {
                logger.error("Bridge Client - Exception: " + throwable.getMessage(), throwable);
                ebnb.stop();
                connected = false;
            });
//            tenant = new CertInfo("C:\\Sviluppo\\Certificati-SSL\\cmroma.it\\cmroma.it.crt").getTenant();
            netSocket.write(tenant + "\n");
            netSocket.write("START SESSION" + "\n");
            netSocket.pause();

//            EventBusNetBridge ebnb = new EventBusNetBridge(netSocket, vertx.eventBus(), address);
            ebnb.setTenant(tenant);
            ebnb.start();
            logger.info("Bridge Client - bridgeUUID: "+ ebnb.getBridgeUUID());

            netSocket.resume();
        } else {
            connected = false;
            String msg = "Bridge Client - not connected to server [" + remoteBridgeHost + ":" + remoteBridgePort +"]";
            Throwable e = netSocketAsyncResult.cause();
            if (e != null) {
                logger.error(msg, e);
            } else {
                logger.error(msg);
            }
        }
    }


    @Override
    public void stop() throws Exception {
        vertx.cancelTimer(connectionTimerID);
        connected = false;
    }

}
