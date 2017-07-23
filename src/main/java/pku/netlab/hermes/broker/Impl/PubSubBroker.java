package pku.netlab.hermes.broker.Impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.Charsets;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.DisconnectMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessageWithKey;
import pku.netlab.hermes.broker.CoreProcessor;
import pku.netlab.hermes.broker.MQTTSession;
import pku.netlab.hermes.parser.MQTTEncoder;
import pku.netlab.hermes.pushservice.PushTask;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by hult on 2017/7/22.
 */
public class PubSubBroker extends CoreProcessor{
    private final Logger logger = LoggerFactory.getLogger(PublishMessage.class);


    @Override
    public void onCreatingSession(String clientID, Handler<AsyncResult<Void>> handler){
        sessionStore.brokerOfClient(clientID, get-> {
            logger.info("DEBUG: " + clientID + get.result());
            if (brokerID.equals(get.result())) {
                if (localSessionMap.get(clientID) != null) {
                    logger.info(clientID + " login from a new TCP connection");
                    try {
                        serverVertx.eventBus().send(clientID, encoder.enc(new DisconnectMessage()));
                    } catch(Exception e){
                        e.printStackTrace();
                    }
                } else {
                    localSessionMap.put(clientID, brokerID);
                }
                sessionStore.addClient(brokerID, clientID, handler);
            } else {
                //TODO: need broadcast clientID among cluster, let disconnect this Client from another broker
                localSessionMap.put(clientID, brokerID);
                sessionStore.addClient(brokerID, clientID, handler);
            }
        });
    }

    @Override
    public void onSessionCreated(MQTTSession session) {
        //TODO: decide if need to send pending message to client
    }

    @Override
    public void clientLogout(String clientID){
        localSessionMap.remove(clientID);
        sessionStore.removeClient(brokerID, clientID, aVoid->{});
    };

    @Override
    public void saveMessage(AbstractMessage message, Handler<String> handler) throws Exception {
        messageStore.save(encoder.enc(message).toString(Charsets.UTF_8), handler);
    };

    @Override
    public void delMessage(String key, String clientID) {
    }

    @Override
    public void delMessage(String key){
        sessionStore.removeMessage(key);
    };

    @Override
    public String getClientSession(String clientID){
        return localSessionMap.get(clientID);
    }

    @Override
    public void updateClientStatus(String clientID) {

    }

    @Override
    public void handleMsgFromMQ(byte[] byteMsg){
        /*
        JsonObject value = new JsonObject(msg.getString("value"));
        try {
            PendingMessage pending = new PendingMessage(value);
            logger.info("pending message: " + pending);
            PublishMessage publish = new PublishMessage();
            publish.setPayload(pending.msg);
            publish.setQos(AbstractMessage.QOSType.LEAST_ONE);
            //a random message id is OK since it will be rewritten in MQTTSession
            publish.setMessageID((int)System.currentTimeMillis() % 65536);
            for (Object o: pending.targets) {
                String client = (String) o;
                if (sessionLocalMap.get(client) == null) {
                    logger.error("cannot find " + client + " in sessionLocalMap");
                }
                publish.setTopicName(client);
                brokerEB.send(client, encoder.enc(publish));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        */
    }

    @Override
    public void onPubAck(String client, String messageID) {
        //TODO: report to MessageQueue
        logger.info("DEBUG: " + client +  messageID);
    }


    @Override
    public void getPendingMessages(String clientID, Handler<List<PublishMessageWithKey>> handler) {
    }

    @Override
    public void push(PushTask.UniPush task, Handler<JsonObject> handler) throws Exception {
        DeliveryOptions options = new DeliveryOptions().addHeader("uniqID", task.uniqueMsgID);
        PublishMessage pub = new PublishMessage();
        pub.setTopicName(task.target);
        pub.setPayload(task.message);
        pub.setQos(AbstractMessage.QOSType.LEAST_ONE);
        pub.setMessageID(345);
        MQTTEncoder encoder = new MQTTEncoder();
        serverVertx.eventBus().send(task.target, encoder.enc(pub), options);
        JsonObject res = new JsonObject();
        res.put("status_code", 0);
        handler.handle(res);
    }

    @Override
    public void pushToMany(PushTask.UniPush task, Handler<JsonObject> handler) {

    }

    @Override
    public void broadcast(PushTask.UniPush task, Handler<JsonObject> handler) {

    }
}
