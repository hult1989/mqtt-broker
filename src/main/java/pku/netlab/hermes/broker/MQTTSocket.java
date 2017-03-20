package pku.netlab.hermes.broker;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import org.dna.mqtt.moquette.proto.messages.*;
import pku.netlab.hermes.MQTTPacketTokenizer;
import pku.netlab.hermes.QOSUtils;
import pku.netlab.hermes.parser.MQTTDecoder;
import pku.netlab.hermes.parser.MQTTEncoder;

import java.nio.ByteBuffer;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.*;

/**
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSocket implements MQTTPacketTokenizer.MqttTokenizerListener, Handler<Message<Buffer>> {

    private static Logger logger = LoggerFactory.getLogger(MQTTSocket.class);

    private Vertx vertx;
    private MQTTSession session;
    private CoreProcessor m_processor;
    private EventBus localServerEB;

    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private MQTTPacketTokenizer tokenizer;
    private NetSocket netSocket;
    private long keepAliveTimerID = -1;
    private boolean keepAliveTimeEnded;
    private Handler<String> keepAliveErrorHandler;
    private MessageConsumer<Buffer> consumer;

    public MQTTSocket(Vertx vertx, NetSocket netSocket, CoreProcessor processor) {
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.tokenizer = new MQTTPacketTokenizer();
        this.tokenizer.registerListener(this);
        this.vertx = vertx;
        this.netSocket = netSocket;
        this.m_processor = processor;
        this.localServerEB = vertx.eventBus();
    }

    public void start() {
        netSocket.setWriteQueueMaxSize(500);
        netSocket.handler(buf-> {
            tokenizer.process(buf.getBytes());
        });
        netSocket.exceptionHandler(event -> {
            String clientInfo = getClientInfo();
            logger.error(clientInfo + ", net-socket exception caught: " + netSocket.writeHandlerID() + " error: " + event.getMessage(), event.getCause());
            clean();
        });
        netSocket.closeHandler(aVoid -> {
            String clientInfo = getClientInfo();
            logger.info(clientInfo + ", net-socket closed ... " + netSocket.writeHandlerID());
            clean();
        });
    }



    public void sendBytesOverSocket(Buffer bytes) {
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
    public void closeConnection() {
        stopKeepAliveTimer();
        netSocket.close();
    }

    private void clean() {
        if(tokenizer!=null) {
            tokenizer.removeAllListeners();
            tokenizer = null;
        }
        if(session!=null) {
            session.shutdown();
            session = null;
        }
        if (this.consumer != null) {
            consumer.unregister();
            this.consumer = null;
        }
        vertx = null;
        m_processor = null;
    }

    @Override
    public void onToken(byte[] token, boolean timeout) throws Exception {
        try {
            if(!timeout) {
                Buffer buffer = Buffer.buffer(token);
                AbstractMessage message = decoder.dec(buffer);
                onMessageFromClient(message);
            } else {
                logger.warn("Timeout occurred ...");
            }
        } catch (Throwable ex) {
            String clientInfo = getClientInfo();
            logger.error(clientInfo +", Bad error in processing the message", ex);
            closeConnection();
        }
    }

    @Override
    public void onError(Throwable e) {
        String clientInfo = getClientInfo();
        logger.error(clientInfo +", "+ e.getMessage(), e);
            closeConnection();
    }

    private void onMessageFromClient(AbstractMessage msg) throws Exception {
        logger.info("Broker <<< " + getClientInfo() + " :" + msg);
        switch (msg.getMessageType()) {
            case CONNECT:

                ConnectMessage connect = (ConnectMessage)msg;
                ConnAckMessage connAck = new ConnAckMessage();
                if (session != null) {
                    logger.warn(String.format("duplicate CONNECT from an existing session %s at %s\n",
                            connect.getClientID(), netSocket.remoteAddress()));
                    /*
                     The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client
                      */
                    connAck.setSessionPresent(true);// TODO implement cleanSession=false
                    //closeConnection();
                    connAck.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
                    sendMessageToClient(connAck);
                } else {
                    /**

                    DBClient.getRedisClient().hgetall("user:" + connect.getClientID(), hgetall -> {
                        if (hgetall.failed()) {
                            logger.warn("failed to process CONNECT, failed to hget from redis because of " + hgetall.cause().getMessage());
                            clean();
                            System.exit(0);
                        } else {
                            String existingServerAddr = hgetall.result().getString("serveraddr");
                            String existingClientAddr = hgetall.result().getString("clientaddr");
                            //TODO 检查shutdown部分的逻辑
                            if (remoteAddr.equals(existingClientAddr) && localAddr.equals(existingServerAddr)) {
                                //碰巧上次的地址和这次新建立的连接的地址相同
                                // 可能是上次的Redis记录尚未清除，且新的client地址又同旧的client地址重复
                                logger.warn(String.format("rare CONNECT from %s at DUPLICATE ADDERSS %s\n", connect.getClientID(), this.remoteAddr));
                            } else if (!localAddr.equals(existingServerAddr)) {
                                //TODO 客户端之前登录在另一台机器上，需要到那台机器上关闭连接
                            } else if (localAddr.equals(existingServerAddr) && !remoteAddr.equals(existingClientAddr)) {
                                //客户与本机建立了一条新的连接，需要关闭旧的连接
                                logger.warn("trying to remove " + existingClientAddr);
                                //需要关闭在本机上之前的连接，但在关闭的方法中一定要判断clientid，避免错误地关闭属于另一个客户socket
                                //关闭时，一定要把session从eventBus中移除
                                //这里以clientid作为关闭的命令
                                vertx.eventBus().send(existingClientAddr, connect.getClientID());
                                logger.info(String.format("ADDRESS UPDATE [%s -> %s] ==> [%s -> %s]", existingClientAddr, existingServerAddr, remoteAddr, localAddr));
                            }
                            //更新redis中用户在线状态记录，TODO 使用方法，封装两个操作
                    DBClient.getRedisClient().hset("user:" + connect.getClientID(), "clientaddr", remoteAddr, null);
                    DBClient.getRedisClient().hset("user:" + connect.getClientID(), "serveraddr", localAddr, null);
                     */
                    String clientID = connect.getClientID();
                    this.session = new MQTTSession(this, m_processor);
                    this.session.setPublishMessageHandler(this::sendMessageToClient);

                    m_processor.clientLogin(clientID, aVoid-> {
                        if (aVoid.succeeded()) {
                            setKeepAliveErrorHandler(cinfo -> {
                                if (session != null) {
                                    cinfo = session.getClientID();
                                }
                                logger.warn("keep alive exhausted! closing connection for client[" + cinfo + "] ...");
                                closeConnection();
                            });

                            connAck.setSessionPresent(false);
                            try {
                                session.handleConnectMessage(connect);
                                this.consumer = localServerEB.consumer(clientID, this);
                                this.consumer.completionHandler(reg-> {
                                    if (reg.succeeded()) {
                                        logger.info(clientID + " registered to localEB");
                                    } else {
                                        logger.error(clientID + " failed to register to localEB");
                                    }
                                });
                                connAck.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
                                sendMessageToClient(connAck);
                                startKeepAliveTimer(connect.getKeepAlive());
                                m_processor.getPendingMessages(clientID, lists -> {
                                    logger.info("try to get pending messages for " + clientID);
                                    for (PublishMessageWithKey pub : lists) {
                                        pub.setTopicName(clientID);
                                        session.handlePublishMessageWithKey(pub);
                                    }
                                });
                            } catch (Exception e) {
                                e.printStackTrace();
                                logger.warn("session failed to process CONNECT because " + e.getMessage());
                                clean();
                            }
                        } else {
                            logger.info("failed to write to Redis");
                            closeConnection();
                        }
                });
            }
            break;
            case SUBSCRIBE:
                resetKeepAliveTimer();

                SubscribeMessage subscribeMessage = (SubscribeMessage)msg;
                session.handleSubscribeMessage(subscribeMessage);
                SubAckMessage subAck = new SubAckMessage();
                subAck.setMessageID(subscribeMessage.getMessageID());
                for(SubscribeMessage.Couple c : subscribeMessage.subscriptions()) {
                    QOSType qos = new QOSUtils().toQos(c.getQos());
                    subAck.addType(qos);
                }
                if(subscribeMessage.isRetainFlag()) {
                    /*
                    When a new subscription is established on a topic,
                    the last retained message on that topic should be sent to the subscriber with the Retain flag set.
                    If there is no retained message, nothing is sent
                    */
                }
                sendMessageToClient(subAck);
                break;
            case UNSUBSCRIBE:
                resetKeepAliveTimer();

                UnsubscribeMessage unsubscribeMessage = (UnsubscribeMessage)msg;
                session.handleUnsubscribeMessage(unsubscribeMessage);
                UnsubAckMessage unsubAck = new UnsubAckMessage();
                unsubAck.setMessageID(unsubscribeMessage.getMessageID());
                sendMessageToClient(unsubAck);
                break;
            case PUBLISH:
                resetKeepAliveTimer();
                PublishMessage publish = (PublishMessage)msg;
                ByteBuffer buffer = ((PublishMessage) msg).getPayload();
                //logger.info(Event.fromByteBuffer(buffer).toString());
                m_processor.enqueKafka(publish);
                switch (publish.getQos()) {
                    case MOST_ONE:
                        break;
                    case LEAST_ONE:
                        PubAckMessage pubAck = new PubAckMessage();
                        pubAck.setMessageID(publish.getMessageID());
                        sendMessageToClient(pubAck);
                        break;
                    case EXACTLY_ONCE:
                        PubRecMessage pubRec = new PubRecMessage();
                        pubRec.setMessageID(publish.getMessageID());
                        sendMessageToClient(pubRec);
                        break;
                }
                break;
            case PUBREC:
                resetKeepAliveTimer();

                PubRecMessage pubRec = (PubRecMessage)msg;
                PubRelMessage prelResp = new PubRelMessage();
                prelResp.setMessageID(pubRec.getMessageID());
                prelResp.setQos(QOSType.LEAST_ONE);
                sendMessageToClient(prelResp);
                break;
            case PUBCOMP:
                resetKeepAliveTimer();
                break;
            case PUBREL:
                resetKeepAliveTimer();
                PubRelMessage pubRel = (PubRelMessage)msg;
                PubCompMessage pubComp = new PubCompMessage();
                pubComp.setMessageID(pubRel.getMessageID());
                sendMessageToClient(pubComp);
                break;
            case PUBACK:
                resetKeepAliveTimer();
                session.handlePublishAck((PubAckMessage)msg);
                break;
            case PINGREQ:
                resetKeepAliveTimer();
                PingRespMessage pingResp = new PingRespMessage();
                sendMessageToClient(pingResp);
                break;
            case DISCONNECT:
                resetKeepAliveTimer();
                DisconnectMessage disconnectMessage = (DisconnectMessage)msg;
                handleDisconnect(disconnectMessage);
                closeConnection();
                break;
            default:
                logger.warn("type of message not known: "+ msg.getClass().getSimpleName());
                break;
        }
    }


    private void sendMessageToClient(AbstractMessage message) {
        try {
            logger.info(">>> " + message);
            Buffer b1 = encoder.enc(message);
            sendBytesOverSocket(b1);
        } catch(Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void handleDisconnect(DisconnectMessage disconnectMessage) {
        session.handleDisconnect(disconnectMessage);
        session = null;
    }


    protected String getClientInfo() {
        String clientInfo = "Session n/a";
        if(session != null) {
            clientInfo = session.getClientInfo();
        }
        return clientInfo;
    }


    private void startKeepAliveTimer(int keepAliveSeconds) {
        if (keepAliveSeconds > 0) {
//            stopKeepAliveTimer();
            keepAliveTimeEnded = true;
        /*
         * If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
         * within one and a half times the Keep Alive time period, it MUST disconnect
         */
            long keepAliveMillis = keepAliveSeconds * 1500;
            keepAliveTimerID = vertx.setPeriodic(keepAliveMillis, tid -> {
                if (keepAliveTimeEnded) {
                    logger.info("keep-alive timer end " + getClientInfo());
                    //should cancel timer first since close connection will set vertx to null
                    stopKeepAliveTimer();
                    if (keepAliveErrorHandler != null && session != null) {
                        keepAliveErrorHandler.handle(session.toString());
                    }
                }
                // next time, will close connection
                keepAliveTimeEnded = true;
            });
        }
    }
    private void stopKeepAliveTimer() {
        if (keepAliveTimerID == -1) return;
        try {
            logger.info("keep-alive cancel old timer: " + keepAliveTimerID + " " + getClientInfo());
            boolean removed = vertx.cancelTimer(keepAliveTimerID);
            if (!removed) {
                logger.warn("keep-alive cancel old timer not removed ID: " + keepAliveTimerID + " " + getClientInfo());
            }
            keepAliveTimerID = -1;
        } catch(Throwable e) {
            logger.error("Cannot stop keep-alive timer with ID: "+keepAliveTimerID +" "+ getClientInfo(), e);
        }
    }
    private void setKeepAliveErrorHandler(Handler<String> handler) {
        this.keepAliveErrorHandler = handler;
    }

    public void resetKeepAliveTimer() {
        keepAliveTimeEnded = false;
    }

    @Override
    public void handle(Message<Buffer> ebMsg) {
        try {
            AbstractMessage msg = this.decoder.dec(ebMsg.body());
            logger.info("from internal publish: " + msg);
            switch (msg.getMessageType()) {
                case DISCONNECT:
                    closeConnection();
                    ebMsg.reply("true");
                    break;
                case PUBLISH:
                    session.handlePublishMessage((PublishMessage)msg);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return String.format(netSocket.remoteAddress() + "<=>" + netSocket.localAddress());
    }
}
