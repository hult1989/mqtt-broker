package pku.netlab.hermes.broker;

import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage.QOSType;
import org.dna.mqtt.moquette.proto.messages.*;
import pku.netlab.hermes.persistence.Subscription;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession {

    private static Logger logger = LoggerFactory.getLogger(MQTTSession.class);

    private MQTTSocket clientSocket;
    private String clientID;
    private boolean cleanSession;
    private Handler<PublishMessage> publishMessageHandler;
    private CoreProcessor m_processor;
    private int msgID;
    private HashMap<Integer, String> inFlightMsgs;


    public MQTTSession(MQTTSocket socket, CoreProcessor processor) {
        this.clientSocket = socket;
        this.m_processor = processor;
        this.msgID = -1;
        this.inFlightMsgs = new HashMap<>();
    }

    public void setPublishMessageHandler(Handler<PublishMessage> publishMessageHandler) {
        this.publishMessageHandler = publishMessageHandler;
    }

    public void handlePublishAck(PubAckMessage ack) {
        inFlightAcknowledged(ack.getMessageID());
    }

    private int nextMessageID() {
        msgID = (1 + msgID) % 65536;
        return msgID;
    }

    private void inFlightAcknowledged(int messageID) {
        String key = inFlightMsgs.remove(msgID);
        m_processor.delMessage(key, clientID);
    }


    public void handleConnectMessage(ConnectMessage connectMessage) throws Exception {

        this.clientID = connectMessage.getClientID();
        this.cleanSession = connectMessage.isCleanSession();
        _handleConnectMessage(connectMessage);
    }


    private void _handleConnectMessage(ConnectMessage connectMessage) {
        if (!cleanSession) {
            logger.info("cleanSession=false: restore old session state with subscriptions ...");
        } else {
        }
        logger.info(this.toString());
    }


    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
    }


    //call this method to publish message to client
    /*
    public void handlePublishMessage(PublishMessage pub) {
        context.runOnContext(v-> m_handlePublishMessage(pub));
    }
    */


    public  void handlePublishMessage(PublishMessage publishMessage) {
        QOSType originalQos = publishMessage.getQos();
        if (originalQos == QOSType.LEAST_ONE) {
            publishMessage.setMessageID(nextMessageID());
        }
        sendPublishMessage(publishMessage);
    }

    public  void handlePublishMessageWithKey(PublishMessageWithKey pub) {
        this.handlePublishMessage(pub);
        inFlightMsgs.put(pub.getMessageID(), pub.getGlobalKey());
    }

    private List<Subscription> getAllMatchingSubscriptions(String topic) {
        return new LinkedList<>();
    }

    public void sendPublishMessage(PublishMessage pm) {
        if(publishMessageHandler!=null)
            publishMessageHandler.handle(pm);
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
        //TODO if not clean session, in flight messages should be saved to database
        if (m_processor != null) {
            m_processor.clientLogout(clientID);
        }
        logger.info(this.toString() + " will shut down, removed from online sessions");
        if (this.clientSocket != null) {
            this.clientSocket.closeConnection();
        }
        this.clientSocket = null;
        this.m_processor = null;
        // stop timers
    }


    public String getClientID() {
        return this.clientID;
    }

    public String getClientInfo() {
        return clientID;
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

    @Override
    public String toString() {
        return String.format("[%s] at [%s]", clientID, clientSocket.toString());
    }
}

