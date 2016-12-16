package io.github.giovibal.mqtt;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;

/**
 * Created by giovanni on 07/05/2014.
 */
public class MQTTNetSocket extends MQTTSocket {

    private static Logger logger = LoggerFactory.getLogger(MQTTNetSocket.class);

    private NetSocket netSocket;

    public MQTTNetSocket(Vertx vertx, ConfigParser config, NetSocket netSocket) {
        super(vertx, config);
        this.netSocket = netSocket;
        this.remoteAddr = netSocket.remoteAddress().toString();
        this.localAddr = netSocket.localAddress().toString();
    }

    public void start() {
        netSocket.setWriteQueueMaxSize(1000);
        netSocket.handler(this);
        netSocket.exceptionHandler(event -> {
            String clientInfo = getClientInfo();
            logger.error(clientInfo + ", net-socket exception caught: " + netSocket.writeHandlerID() + " error: " + event.getMessage(), event.getCause());
            //event.getCause().printStackTrace();
            handleWillMessage();
            shutdown();
        });
        netSocket.closeHandler(aVoid -> {
            String clientInfo = getClientInfo();
            logger.info(clientInfo + ", net-socket closed ... " + netSocket.writeHandlerID());
            handleWillMessage();
            shutdown();

        });
    }


    @Override
    protected void sendMessageToClient(Buffer bytes) {
        try {
            netSocket.write(bytes);
            if (netSocket.writeQueueFull()) {
                netSocket.pause();
                netSocket.drainHandler( done -> netSocket.resume() );
            }
        } catch(Throwable e) {
            logger.error(e.getMessage());
        }
    }

    protected void closeConnection() {
        netSocket.close();
    }

}
