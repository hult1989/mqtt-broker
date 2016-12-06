package io.github.giovibal.mqtt;

import io.github.giovibal.mqtt.parser.MQTTDecoder;
import io.github.giovibal.mqtt.parser.MQTTEncoder;
import io.github.giovibal.mqtt.persistence.StoreManager;
import io.github.giovibal.mqtt.persistence.Subscription;
import io.github.giovibal.mqtt.pushservice.DBClient;
import io.github.giovibal.mqtt.pushservice.PushMessage;
import io.github.giovibal.mqtt.security.AuthorizationClient;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import org.dna.mqtt.moquette.proto.messages.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by giovanni on 07/05/2014.
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTSession implements Handler<Message<Buffer>> {

    private static Logger logger = LoggerFactory.getLogger(MQTTSession.class);

    public static final String ADDRESS = "io.github.giovibal.mqtt";
    public static final String TENANT_HEADER = "tenant";
//    public static final String AUTHORIZATION_ADDRESS = "io.github.giovibal.mqtt.OAuth2AuthenticatorVerticle";

    private Vertx vertx;
    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private ITopicsManager topicsManager;
    private String clientID;
    private String protoName;
    private boolean cleanSession;
    private String tenant;
    private boolean securityEnabled;
    private String authenticatorAddress;
    private boolean retainSupport;
    private MessageConsumer<Buffer> messageConsumer;
    private Handler<PublishMessage> publishMessageHandler;
    private Map<String, Subscription> subscriptions;
    private QOSUtils qosUtils;
    private StoreManager storeManager;
    private Map<String, List<Subscription>> matchingSubscriptionsCache;
    private PublishMessage willMessage;

//    private int keepAliveSeconds;
    private long keepAliveTimerID = -1;
    private boolean keepAliveTimeEnded;
    private Handler<String> keepaliveErrorHandler;
    private MessageConsumer<Buffer> pushConsumer;

    public MQTTSession(Vertx vertx, ConfigParser config) {
        this.vertx = vertx;
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.securityEnabled = config.isSecurityEnabled();
        this.retainSupport = config.isRetainSupport();
        this.subscriptions = new LinkedHashMap<>();
        this.qosUtils = new QOSUtils();
        this.matchingSubscriptionsCache = new HashMap<>();

        this.topicsManager = new MQTTTopicsManagerOptimized();
        this.storeManager = new StoreManager(this.vertx);
        this.authenticatorAddress = config.getAuthenticatorAddress();

    }

    private String extractTenant(String username) {
        if(username == null || username.trim().length()==0)
            return "";
        String tenant = "";
        int idx = username.lastIndexOf('@');
        if(idx > 0) {
            tenant = username.substring(idx+1);
        }
        return tenant;
    }

    public void setPublishMessageHandler(Handler<PublishMessage> publishMessageHandler) {
        this.publishMessageHandler = publishMessageHandler;
    }

    public void setKeepaliveErrorHandler(Handler<String> keepaliveErrorHandler) {
        this.keepaliveErrorHandler = keepaliveErrorHandler;
    }

    public PublishMessage getWillMessage() {
        return willMessage;
    }

    //@hult
    public void handleConnectMessage(ConnectMessage connectMessage,
                                     Handler<Boolean> authHandler)
            throws Exception {

        clientID = connectMessage.getClientID();
        cleanSession = connectMessage.isCleanSession();
        protoName = connectMessage.getProtocolName();
        if("MQIsdp".equals(protoName)) {
            logger.debug("Detected MQTT v. 3.1 " + protoName + ", clientID: " + clientID);
        } else if("MQTT".equals(protoName)) {
            logger.debug("Detected MQTT v. 3.1.1 " + protoName + ", clientID: " + clientID);
        } else {
            logger.debug("Detected MQTT protocol " + protoName + ", clientID: " + clientID);
        }

        //用户登陆后在这里订阅自己的消息
        this.registerPushService(clientID);



        String username = connectMessage.getUsername();
        String password = connectMessage.getPassword();

        if(securityEnabled) {
            AuthorizationClient auth = new AuthorizationClient(vertx.eventBus(), authenticatorAddress);
            auth.authorize(username, password, validationInfo -> {
                if (validationInfo.auth_valid) {
                    String tenant = validationInfo.tenant;
                    _initTenant(tenant);
                    _handleConnectMessage(connectMessage);
                    authHandler.handle(Boolean.TRUE);
                } else {
                    authHandler.handle(Boolean.FALSE);
                }
            });
        }
        else {
            String clientID = connectMessage.getClientID();
            String tenant = null;
            if(username == null || username.trim().length()==0) {
                tenant = extractTenant(clientID);
            }
            else {
                tenant = extractTenant(username);
            }
            _initTenant(tenant);
            _handleConnectMessage(connectMessage);
            authHandler.handle(Boolean.TRUE);
        }
    }

    //TODO: 这样似乎是不够的，因为没法确定客户端会如何处理自己未订阅的消息
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


    private void _initTenant(String tenant) {
        if(tenant == null)
            throw new IllegalStateException("Tenant cannot be null");
        this.tenant = tenant;
    }
    private void _handleConnectMessage(ConnectMessage connectMessage) {
        if (!cleanSession) {
            logger.debug("cleanSession=false: restore old session state with subscriptions ...");
        }
        boolean isWillFlag = connectMessage.isWillFlag();
        if(isWillFlag) {
            String willMessageM = connectMessage.getWillMessage();
            String willTopic = connectMessage.getWillTopic();
            byte willQosByte = connectMessage.getWillQos();
            AbstractMessage.QOSType willQos = qosUtils.toQos(willQosByte);

            try {
                willMessage = new PublishMessage();
                willMessage.setPayload(willMessageM);
                willMessage.setTopicName(willTopic);
                willMessage.setQos(willQos);
                switch (willQos) {
                    case EXACTLY_ONCE:
                    case LEAST_ONE:
                        willMessage.setMessageID(1);
                }
            } catch (UnsupportedEncodingException e) {
                logger.error(e.getMessage(), e);
            }
        }

        startKeepAliveTimer(connectMessage.getKeepAlive());
        logger.info("New connection client : " + getClientInfo());
    }

    private void startKeepAliveTimer(int keepAliveSeconds) {
        if(keepAliveSeconds > 0) {
//            stopKeepAliveTimer();
            keepAliveTimeEnded = true;
            /*
             * If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
             * within one and a half times the Keep Alive time period, it MUST disconnect
             */
            long keepAliveMillis = keepAliveSeconds * 1500;
            keepAliveTimerID = vertx.setPeriodic(keepAliveMillis, tid -> {
                if(keepAliveTimeEnded) {
                    logger.debug("keep-alive timer end " + getClientInfo());
                    handleWillMessage();
                    if (keepaliveErrorHandler != null) {
                        keepaliveErrorHandler.handle(clientID);
                    }
                    stopKeepAliveTimer();
                }
                // next time, will close connection
                keepAliveTimeEnded = true;
            });
        }
    }
    private void stopKeepAliveTimer() {
        try {
            logger.debug("keep-alive cancel old timer: " + keepAliveTimerID + " " + getClientInfo());
            boolean removed = vertx.cancelTimer(keepAliveTimerID);
            if (!removed) {
                logger.warn("keep-alive cancel old timer not removed ID: " + keepAliveTimerID + " " + getClientInfo());
            }
        } catch(Throwable e) {
            logger.error("Cannot stop keep-alive timer with ID: "+keepAliveTimerID +" "+ getClientInfo(), e);
        }
    }

    public void resetKeepAliveTimer() {
        keepAliveTimeEnded = false;
    }


    //reviewed by hult @2016-10-24
    public void handlePublishMessage(PublishMessage publishMessage) {
        System.out.println(publishMessage.getTopicName());
        //如果是司机发布的自定义的地理位置消息，没有必要进行交给mqtt的业务逻辑处理，自己交个locationHandler写入MongoDB即可
        if (publishMessage.getTopicName().equals("locations")) {
            try {
                vertx.eventBus().send("locations", this.encoder.enc(publishMessage));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return;
        }
        try {
            // publish always have tenant, if session is not tenantized, tenant is retrieved from topic ([tenant]/to/pi/c)
            String publishTenant = calculatePublishTenant(publishMessage);
            System.out.println(publishTenant);

            // store retained messages ...
            if(publishMessage.isRetainFlag()) {
                boolean payloadIsEmpty=false;
                ByteBuffer bb = publishMessage.getPayload();
                if(bb!=null) {
                    byte[] bytes = bb.array();
                    if (bytes.length == 0) {
                        payloadIsEmpty = true;
                    }
                }
                if(payloadIsEmpty) {
                    storeManager.deleteRetainMessage(publishTenant, publishMessage.getTopicName());
                } else {
                    storeManager.saveRetainMessage(publishTenant, publishMessage);
                }
            }

            /* It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent to a Client
             * because it matches an established subscription
             * regardless of how the flag was set in the message it received. */
            publishMessage.setRetainFlag(false);
            Buffer msg = encoder.enc(publishMessage);
            if(tenant == null)
                tenant = "";
            DeliveryOptions opt = new DeliveryOptions().addHeader(TENANT_HEADER, publishTenant);
            vertx.eventBus().send(ADDRESS, msg, opt);

//            NOT TESTED... It's only a code sample trying to resolve "No pong from server" error messages in production ...
//            MessageProducer<Buffer> producer = vertx.eventBus().publisher(ADDRESS);
//            producer.deliveryOptions(opt);
//            producer.write(msg);
//            if (producer.writeQueueFull()) {
////                producer.pause();
////                producer.drainHandler( done -> producer.resume());
//            }

        } catch(Throwable e) {
            logger.error(e.getMessage());
        }
    }

    private String calculatePublishTenant(PublishMessage publishMessage) {
        return calculatePublishTenant(publishMessage.getTopicName());
    }
    private String calculatePublishTenant(String topic) {
        boolean isTenantSession = isTenantSession();
        if(isTenantSession) {
            return tenant;
        } else {
            String t;
            boolean slashFirst = topic.startsWith("/");
            if (slashFirst) {
                int idx = topic.indexOf('/', 1);
                if(idx>1)
                    t = topic.substring(1, idx);
                else
                    t = topic.substring(1);
            } else {
                int idx = topic.indexOf('/', 0);
                if(idx>0)
                    t = topic.substring(0, idx);
                else
                    t = topic;
            }
            return t;
        }
    }

    public void handleSubscribeMessage(SubscribeMessage subscribeMessage) {
        try {
            final int messageID = subscribeMessage.getMessageID();
            if(this.messageConsumer==null) {
                messageConsumer = vertx.eventBus().consumer(ADDRESS);
                messageConsumer.handler(this);
            }

            // invalidate matching topic cache
            matchingSubscriptionsCache.clear();

            List<SubscribeMessage.Couple> subs = subscribeMessage.subscriptions();
            for(SubscribeMessage.Couple s : subs) {
                String topicFilter = s.getTopicFilter();
                Subscription sub = new Subscription();
                sub.setQos(s.getQos());
                sub.setTopicFilter(topicFilter);
                this.subscriptions.put(topicFilter, sub);

                String publishTenant = calculatePublishTenant(topicFilter);

                // check in client wants receive retained message by this topicFilter
                if(retainSupport) {
                    storeManager.getRetainedMessagesByTopicFilter(publishTenant, topicFilter, (List<PublishMessage> retainedMessages) -> {
                        if (retainedMessages != null) {
                            int incrMessageID = messageID;
                            for (PublishMessage retainedMessage : retainedMessages) {
                                switch (retainedMessage.getQos()) {
                                    case LEAST_ONE:
                                    case EXACTLY_ONCE:
                                        retainedMessage.setMessageID(++incrMessageID);
                                }
                                retainedMessage.setRetainFlag(true);
                                handlePublishMessageReceived(retainedMessage);
                            }
                        }
                    });
                }
            }
        } catch(Throwable e) {
            logger.error(e.getMessage());
        }
    }

    private boolean isTenantSession() {
        boolean isTenantSession = tenant!=null && tenant.trim().length()>0;
        return isTenantSession;
    }
    private boolean tenantMatch(Message<Buffer> message) {
        boolean isTenantSession = isTenantSession();
        boolean tenantMatch;
        if(isTenantSession) {
            boolean containsTenantHeader = message.headers().contains(TENANT_HEADER);
            if (containsTenantHeader) {
                String tenantHeaderValue = message.headers().get(TENANT_HEADER);
                tenantMatch =
                        tenant.equals(tenantHeaderValue)
                                || "".equals(tenantHeaderValue)
                ;
            } else {
                // if message doesn't contains header is not for a tenant-session
                tenantMatch = false;
            }
        } else {
            // if this is not a tenant-session, receive all messages from all tenants
            tenantMatch = true;
        }
        return tenantMatch;
    }

    @Override
    public void handle(Message<Buffer> message) {
        try {
            logger.warn(String.format("%s try to handle message %s", this.clientID, message.toString()));
            boolean tenantMatch = tenantMatch(message);
            if(tenantMatch) {
                Buffer in = message.body();
                PublishMessage pm = (PublishMessage) decoder.dec(in);
                // filter messages by of subscriptions of this client
                if(pm == null) {
                    logger.warn("PublishMessage is null, message.headers => "+ message.headers().entries()+"");
                }
                else {
                    handlePublishMessageReceived(pm);
                }
            } else {
                logger.warn("message will be dropt because no tenant match");
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

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
        logger.debug("Disconnect from " + clientID +" ...");
        /*
         * TODO: implement this behaviour
         * On receipt of DISCONNECT the Server:
         * - MUST discard any Will Message associated with the current connection without publishing it, as described in Section 3.1.2.5 [MQTT-3.14.4-3].
         * - SHOULD close the Network Connection if the Client has not already done so.
         */
        shutdown();
    }

    public void shutdown() {
        // stop timers
        stopKeepAliveTimer();

        // deallocate this instance ...
        if (pushConsumer != null) pushConsumer.unregister();
        if(messageConsumer!=null && cleanSession) {
            messageConsumer.unregister();
            messageConsumer = null;
        }
        vertx = null;
    }

    public void handleWillMessage() {
        // publish will message if present ...
        if(willMessage != null) {
            logger.debug("publish will message ... topic[" + willMessage.getTopicName()+"]");
            handlePublishMessage(willMessage);
        }
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
}
