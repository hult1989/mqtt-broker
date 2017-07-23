package pku.netlab.hermes.broker;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage.QOSType;
import org.dna.mqtt.moquette.proto.messages.*;
import pku.netlab.hermes.persistence.Subscription;

import java.util.*;

/**
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession {
    private static Logger logger = LoggerFactory.getLogger(MQTTSession.class);

    private MQTTConnection clientConnection;
    public final String clientID;
    private CoreProcessor m_processor;
    private int msgID;
    private Map<Integer, String> inFlight;
    public final boolean cleanSession;


    public MQTTSession(MQTTConnection conn, CoreProcessor processor, ConnectMessage conMsg) {
        this.clientConnection = conn;
        this.m_processor = processor;
        this.msgID = -1;
        this.inFlight = new HashMap<>();
        this.cleanSession = conMsg.isCleanSession();
        this.clientID = conMsg.getClientID();
        this.clientConnection.startKeepAliveTimer(conMsg.getKeepAlive());
        this.sessionInit();
    }

    public void handlePublishAck(PubAckMessage ack) {
        //TODO: how should message be removed? processor desides
        String uniqID = inFlight.remove(ack.getMessageID());
        m_processor.onPubAck(clientID, uniqID);
    }

    private int nextMessageID() {
        msgID = (1 + msgID) % 65536;
        return msgID;
    }


    private void sessionInit() {
        m_processor.onCreatingSession(clientID, login -> {
            if (login.succeeded()) {
                clientConnection.registerOnEventBus(clientID);

                ConnAckMessage connAck = new ConnAckMessage();
                connAck.setSessionPresent(false);
                connAck.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
                clientConnection.sendMessageToClient(connAck);
                /**
                 * TODO：登录后是否拉取消息进行推送，应该由CoreProcessor决定
                 */
                m_processor.onSessionCreated(this);
            } else {
                shutdown();
            }
        });
    }


    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
    }

    public  void handlePublishMessage(PublishMessage publishMessage) {
        QOSType originalQos = publishMessage.getQos();
        if (originalQos == QOSType.MOST_ONE) {
        } else {
            try {
                m_processor.saveMessage(publishMessage, key -> {
                });
            } catch (Exception e) {
                return;
            }
        }
    }

    private List<Subscription> getAllMatchingSubscriptions(String topic) {
        return new LinkedList<>();
    }


    public void handleUnsubscribeMessage(UnsubscribeMessage unsubscribeMessage) {
    }

    public void handleDisconnect(DisconnectMessage disconnectMessage) {
        logger.info("Disconnect from " + clientID +" ...");
        /*
         * TODO: implement this behaviour
         * On receipt of DISCONNECT the Server:
         * - MUST discard any Will Message associated with the current connection without publishing it, as described in Section 3.1.2.5 [MQTT-3.14.4-3].
         * - SHOULD close the Network Connection if the Client has not already done so.
         */
        shutdown();
    }

    public void shutdown() {
        //TODO: if not clean session, in flight messages should be processed
        if (m_processor != null) {
            m_processor.clientLogout(clientID);
        }
        logger.info(this.toString() + " will shut down, removed from online sessions");
        if (this.clientConnection != null) {
            this.clientConnection.closeConnection();
        }
        this.clientConnection = null;
        this.m_processor = null;
        // stop timers
    }

    //always assume that message have be saved before send to client
    public void publishMessageToClient(PublishMessage pub, String uniqID) {
        if (pub.getQos() == QOSType.LEAST_ONE) {
            int msgId = nextMessageID();
            pub.setMessageID(msgId);
            inFlight.putIfAbsent(msgId, uniqID);
        }
        clientConnection.sendMessageToClient(pub);
    }

    @Override
    public String toString() {
        //TODO：should show how many inflight
        return String.format("[%s] at [%s]", clientID, clientConnection.toString());
    }
}


    /*
    public void handleArchiveMsg() {
        RedisClient redis = DBClient.getRedisClient();
        redis.smembers("mq:" + this.clientID, smembers -> {
            if (smembers.failed()) {
                smembers.cause().printStackTrace();
                logger.warn(Arrays.toString(smembers.cause().getStackTrace()));
            } else if (smembers.result().size() == 0) {
                return;
            } else {
                List<String> msgKeys = new ArrayList<>(smembers.result().getList().size());
                for (Object obj : smembers.result().getList()) {
                    //从消息队列中获取消息seq构成msgKeys作为全局消息id的key
                    msgKeys.add(String.format("%s:%s", this.clientID, obj));
                }
                redis.mgetMany(msgKeys, mget -> {
                    //mget结果为如'580d6c7a97ae811f0551810d'的列表，key为alex:119之类的，映射按照先后顺序一一对应
                    if (mget.failed()) {
                        mget.cause().printStackTrace();
                        logger.warn(Arrays.toString(mget.cause().getStackTrace()));
                    } else {
                        JsonArray query = new JsonArray();
                        HashMap<String, String> glo2cur = new HashMap<>();
                        for (int i = 0; i < mget.result().size(); i += 1) {
                            String msgID = mget.result().getString(i);
                            //构建一个全局消息ID到seq的映射，以便修改消息中的seq，key为msgID,value为mseq
                            glo2cur.put(msgID, msgKeys.get(i).replace(this.clientID + ":", ""));
                            //根据_id:580d6c7a97ae811f0551810d在MongoDB中获取消息
                            query.add(new JsonObject().put("_id", msgID));
                        }
                        DBClient.getMongoClient().find("messages", new JsonObject().put("$or", query), find -> {
                            //一次性地根据全局消息id把所有的消息都取出来
                            if (find.failed()) {
                                find.cause().printStackTrace();
                                logger.warn(Arrays.toString(find.cause().getStackTrace()));
                            } else {
                                for (Object msg : find.result()) {
                                    JsonObject jsonMsg = (JsonObject) msg;
                                    jsonMsg.put("msgID", Integer.valueOf(glo2cur.get(jsonMsg.getString("_id"))));
                                    //移除MongoDB为消息添加的_id字段，根据前面的映射替换为mseq
                                    jsonMsg.remove("_id");
                                    try {
                                        this.vertx.eventBus().publish(jsonMsg.getString("uid"), this.encoder.enc(PushMessage.convert(jsonMsg)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        logger.warn(Arrays.toString(e.getStackTrace()));
                                    }
                                }
                            }
                        });
                    }
                });
            }
        });
    }
    */

