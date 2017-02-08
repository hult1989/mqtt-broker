package pku.netlab.hermes.broker;

import pku.netlab.hermes.ConfigParser;
import pku.netlab.hermes.ITopicsManager;
import pku.netlab.hermes.MQTTTopicsManagerOptimized;
import pku.netlab.hermes.QOSUtils;
import pku.netlab.hermes.persistence.Subscription;
import pku.netlab.hermes.pushservice.DBClient;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.dna.mqtt.moquette.proto.messages.*;

import java.util.*;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession {

    private static Logger logger = LoggerFactory.getLogger(MQTTSession.class);

    public static final String ADDRESS = "MQTT_BROKER_EVENTBUS";
//    public static final String AUTHORIZATION_ADDRESS = "io.github.giovibal.mqtt.OAuth2AuthenticatorVerticle";

    private MQTTSocket clientSocket;
    private ITopicsManager topicsManager;
    private String clientID;
    private String protoName;
    private boolean cleanSession;
    private Handler<PublishMessage> publishMessageHandler;
    private Map<String, Subscription> subscriptions;
    private QOSUtils qosUtils;
    private Map<String, List<Subscription>> matchingSubscriptionsCache;
    private IMessageStore messageStore;
    private ISessionStore sessionStore;


    public MQTTSession(MQTTSocket socket, ConfigParser config) {
        this.clientSocket = socket;
        this.subscriptions = new LinkedHashMap<>();
        this.qosUtils = new QOSUtils();
        this.matchingSubscriptionsCache = new HashMap<>();

        this.topicsManager = new MQTTTopicsManagerOptimized();
        this.messageStore = IMessageStore.getStore("DBStore");
        this.sessionStore = MQTTBroker.getSessionStore();
    }

    public void setPublishMessageHandler(Handler<PublishMessage> publishMessageHandler) {
        this.publishMessageHandler = publishMessageHandler;
    }

    public int nextMessageID() {
        return 0;
    }

    public void inFlightAcknowledged(int messageID) {

    }

    public void enqueue(AbstractMessage message) {

    }




    //@hult
    public void handleConnectMessage(ConnectMessage connectMessage,
                                     Handler<Boolean> authHandler)
            throws Exception {

        this.clientID = connectMessage.getClientID();
        this.cleanSession = connectMessage.isCleanSession();
        this.protoName = connectMessage.getProtocolName();
        if("MQIsdp".equals(protoName)) {
            logger.info("Detected MQTT v. 3.1 " + protoName + ", clientID: " + clientID);
        } else if("MQTT".equals(protoName)) {
            logger.info("Detected MQTT v. 3.1.1 " + protoName + ", clientID: " + clientID);
        } else {
            logger.info("Detected MQTT protocol " + protoName + ", clientID: " + clientID);
        }

        _handleConnectMessage(connectMessage);
        authHandler.handle(Boolean.TRUE);
        this.sessionStore.addClient(MQTTBroker.brokerID,  clientID, res-> {
            if (!res.succeeded()) {
                logger.error("failed to add session for " + res.cause().getMessage());
            } else {
                logger.info("add user [" + clientID + "] to redis");
            }
        });
    }

    //TODO: 这样似乎是不够的，因为没法确定客户端会如何处理自己未订阅的消息
    /*
    private void registerPushService(String clientID) {
        //自定义的方法，每个连接的用户都在eventbus中关注了自己的id，以便从中接受消息
        this.pushConsumer = vertx.eventBus().consumer(clientID);

        logger.info(String.format("%s subscribe %s from eventbus", this.toString(), clientID));
        this.pushConsumer.handler(message -> {
            int maxQos = 2;
            PublishMessage publishMessage = null;
            try {
                publishMessage = (PublishMessage) this.decoder.dec(message.body());
            } catch (Exception e) {
                e.printStackTrace();
            }
            AbstractMessage.QOSType originalQos = publishMessage.getQos();
            int iSentQos = qosUtils.toInt(originalQos);
            int iOkQos = qosUtils.calculatePublishQos(iSentQos, maxQos);
            AbstractMessage.QOSType qos = qosUtils.toQos(iOkQos);
            publishMessage.setQos(qos);
            this.sendPublishMessage(publishMessage);
        });
    }
    */

    private void _handleConnectMessage(ConnectMessage connectMessage) {
        if (!cleanSession) {
            logger.info("cleanSession=false: restore old session state with subscriptions ...");
            messageStore.loadStore();
        } else {
            messageStore.dropAllMessages();
        }
        logger.info("New connection client : " + getClientInfo());
    }




    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
        try {
            final int messageID = subscribeMessage.getMessageID();

            // invalidate matching topic cache
            matchingSubscriptionsCache.clear();

            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for(SubscribeMessage.Couple s : subs) {
                String topicFilter = s.getTopicFilter();
                Subscription sub = new Subscription();
                sub.setQos(s.getQos());
                sub.setTopicFilter(topicFilter);
                this.subscriptions.put(topicFilter, sub);

            }
        } catch(Throwable e) {
            logger.error(e.getMessage());
        }
    }


    //use this method to publish message
    public void handlePublishMessageReceived(PublishMessage publishMessage) {


        boolean publishMessageToThisClient = false;
        int maxQos = -1;

        /*
         * the Server MUST deliver the message to the Client respecting the maximum QoS of all the matching subscriptions
         */
        String topic = publishMessage.getTopicName();
        List<Subscription> subs = getAllMatchingSubscriptions(topic);
        if(subs!=null && subs.size()>0) {
            publishMessageToThisClient = true;
            for (Subscription s : subs) {
                int itemQos = s.getQos();
                if (itemQos > maxQos) {
                    maxQos = itemQos;
                }
                // optimization: if qos==2 is alredy **the max** allowed
                if(maxQos == 2)
                    break;
            }
        }

        if(publishMessageToThisClient) {
            // the qos cannot be bigger than the subscribe requested qos ...
            AbstractMessage.QOSType originalQos = publishMessage.getQos();
            int iSentQos = qosUtils.toInt(originalQos);
            int iOkQos = qosUtils.calculatePublishQos(iSentQos, maxQos);
            AbstractMessage.QOSType qos = qosUtils.toQos(iOkQos);
            publishMessage.setQos(qos);
            sendPublishMessage(publishMessage);
        }
    }

    private List<Subscription> getAllMatchingSubscriptions(String topic) {
        List<Subscription> ret = new ArrayList<>();
//        String topic = pm.getTopicName();
        if(matchingSubscriptionsCache.containsKey(topic)) {
            return matchingSubscriptionsCache.get(topic);
        }
        // check if topic of published message pass at least one of the subscriptions
        for (Subscription c : subscriptions.values()) {
            String topicFilter = c.getTopicFilter();
            boolean match = topicsManager.match(topic, topicFilter);
            if (match) {
                ret.add(c);
            }
        }
        matchingSubscriptionsCache.put(topic, ret);
        return ret;
    }

    private void sendPublishMessage(PublishMessage pm) {
        if(publishMessageHandler!=null)
            publishMessageHandler.handle(pm);
    }

    private void messageEnque(PublishMessage message) {

    }



    public void handleUnsubscribeMessage(UnsubscribeMessage unsubscribeMessage) {
        try {
            List<String> topicFilterSet = unsubscribeMessage.topicFilters();
            for (String topicFilter : topicFilterSet) {
                if(subscriptions!=null) {
                    subscriptions.remove(topicFilter);
                    matchingSubscriptionsCache.clear();
                }
            }
        }
        catch(Throwable e) {
            logger.error(e.getMessage());
        }
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
        MQTTBroker.onlineSessions.remove(clientID);
        this.sessionStore.removeClient(MQTTBroker.brokerID, clientID, res-> {
            if (!res.succeeded()) {
                logger.error("failed to remove user session " + clientID);
            } else {
                logger.info("remove user " + clientID + " session");
            }
        });
        logger.info(this.toString() + " will shut down, removed from online sessions");
        if (this.clientSocket != null) {
            this.clientSocket.closeConnection();
        }
        this.clientSocket = null;
        // stop timers
    }


    public String getClientID() {
        return this.clientID;
    }

    public String getClientInfo() {
        String clientInfo ="clientID: "+ clientID +", MQTT protocol: "+ protoName +"";
        return clientInfo;
    }

    public void handlePushAck(PubAckMessage puback) {
        //从消息队列中移除
        DBClient.getRedisClient().srem("mq:" + this.clientID, String.valueOf(puback.getMessageID()), null);
        //溢出seq到msgID的映射
        DBClient.getRedisClient().del(this.clientID + ":" + puback.getMessageID(), null);
    }

    //@hult 2016-10-24
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
        return String.format("clientID: %s, state: %s", clientID, "SESSION");
    }
}

