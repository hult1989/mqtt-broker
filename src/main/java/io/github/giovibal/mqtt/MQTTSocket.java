package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import org.dna.mqtt.moquette.proto.messages.*;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.*;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSocket implements MQTTPacketTokenizer.MqttTokenizerListener {

    private static Logger logger = LoggerFactory.getLogger(MQTTSocket.class);

    protected Vertx vertx;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private MQTTPacketTokenizer tokenizer;
    protected MQTTSession session;
    private ConfigParser config;
    protected String remoteAddr;
    private ISessionStore sessionStore;
    private NetSocket netSocket;

    public MQTTSocket(Vertx vertx, ConfigParser config, NetSocket netSocket) {
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.tokenizer = new MQTTPacketTokenizer();
        this.tokenizer.registerListener(this);
        this.vertx = vertx;
        this.config = config;
        this.netSocket = netSocket;

    }
    public void start() {
        netSocket.setWriteQueueMaxSize(1000);
        netSocket.handler(buf-> {
            tokenizer.process(buf.getBytes());
        });
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
        netSocket.close();
    }
    public void shutdown() {
        logger.warn("shutdown call");
        if(tokenizer!=null) {
            tokenizer.removeAllListeners();
            tokenizer = null;
        }
        if(session!=null) {
            session.shutdown();
            session = null;
        }
        vertx = null;
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
//        if(e instanceof CorruptedFrameException) {
            closeConnection();
//        }
    }

    private void onMessageFromClient(AbstractMessage msg) throws Exception {
        logger.info("Broker <<< " + getClientInfo() + " :" + msg);
        switch (msg.getMessageType()) {
            case CONNECT:

                /*
                需要处理的情况包括，一个client在同一条链路上发来了多个connect消息，这时需要忽略掉后面的connect
                一个client在一条新的链路上发来了connect消息，这是需要关闭掉前一个链接，清除前一个session，然后重新简历session
                前后两种情况的区别是，两次消息的来源是否是同一条tcp链路
                让每个socket都在eventBus上监听remote address 的地址，方便接受踢下线的消息
                 */
                ConnectMessage connect = (ConnectMessage)msg;
                ConnAckMessage connAck = new ConnAckMessage();
                if (session != null) {
                    logger.warn(String.format("duplicate CONNECT from %s at %s\n", connect.getClientID(), this.remoteAddr));
                    //这里是处理同一个连接上发来了多个CONNECT请求的情况，但是目前还没有处
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
                            shutdown();
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
                    MQTTSocket existedSock = MQTTBroker.onlineClients.get(connect.getClientID());
                    if (existedSock != null) {
                        existedSock.closeConnection();
                    }

                    MQTTBroker.onlineClients.put(connect.getClientID(), this);

                    session = new MQTTSession(vertx, config);
                    session.setPublishMessageHandler(this::sendMessageToClient);
                    session.setKeepaliveErrorHandler(clientID -> {
                        String cinfo = clientID;
                        if (session != null) {
                            cinfo = session.getClientInfo();
                        }
                        logger.info("keep alive exhausted! closing connection for client[" + cinfo + "] ...");
                        closeConnection();
                    });

                    connAck.setSessionPresent(false);
                    try {
                        session.handleConnectMessage(connect, authenticated -> {
                            if (authenticated) {
                                connAck.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
                                sendMessageToClient(connAck);
                                //session.handleArchiveMsg();
                            } else {
                                logger.error("Authentication failed! clientID= " + connect.getClientID() + " username=" + connect.getUsername());
                                connAck.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
                                sendMessageToClient(connAck);
                            }
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.warn("session failed to process CONNECT because " + e.getMessage());
                        shutdown();
                    }
            }
            break;
            case SUBSCRIBE:
                session.resetKeepAliveTimer();

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
                session.resetKeepAliveTimer();

                UnsubscribeMessage unsubscribeMessage = (UnsubscribeMessage)msg;
                session.handleUnsubscribeMessage(unsubscribeMessage);
                UnsubAckMessage unsubAck = new UnsubAckMessage();
                unsubAck.setMessageID(unsubscribeMessage.getMessageID());
                sendMessageToClient(unsubAck);
                break;
            case PUBLISH:
                session.resetKeepAliveTimer();

                PublishMessage publish = (PublishMessage)msg;
                session.handlePublishMessage(publish);
                switch (publish.getQos()) {
                    case RESERVED:
                        break;
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
                session.resetKeepAliveTimer();

                PubRecMessage pubRec = (PubRecMessage)msg;
                PubRelMessage prelResp = new PubRelMessage();
                prelResp.setMessageID(pubRec.getMessageID());
                prelResp.setQos(QOSType.LEAST_ONE);
                sendMessageToClient(prelResp);
                break;
            case PUBCOMP:
                session.resetKeepAliveTimer();
                break;
            case PUBREL:
                session.resetKeepAliveTimer();
                PubRelMessage pubRel = (PubRelMessage)msg;
                PubCompMessage pubComp = new PubCompMessage();
                pubComp.setMessageID(pubRel.getMessageID());
                sendMessageToClient(pubComp);
                break;
            case PUBACK:
                session.resetKeepAliveTimer();
                //session.handlePushAck((PubAckMessage)msg);


                // A PUBACK message is the response to a PUBLISH message with QoS level 1.
                // A PUBACK message is sent by a server in response to a PUBLISH message from a publishing client,
                // and by a subscriber in response to a PUBLISH message from the server.
                break;
            case PINGREQ:
                session.resetKeepAliveTimer();
                PingRespMessage pingResp = new PingRespMessage();
                sendMessageToClient(pingResp);
                break;
            case DISCONNECT:
                session.resetKeepAliveTimer();
                DisconnectMessage disconnectMessage = (DisconnectMessage)msg;
                handleDisconnect(disconnectMessage);
                closeConnection();
                break;
            default:
                logger.warn("type of message not known: "+ msg.getClass().getSimpleName());
                break;
        }

        // TODO: forward mqtt message to backup server

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

    protected void handleWillMessage() {
//        logger.info("handle will message... ");
        if(session != null) {
//            logger.info("handle will message: session found!");
            session.handleWillMessage();
        }
//        logger.info("handle will message end.");
    }

    public void setSessionStore(ISessionStore store) {
        this.sessionStore = store;
    }
}
