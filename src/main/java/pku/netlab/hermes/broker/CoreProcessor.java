package pku.netlab.hermes.broker;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import org.apache.commons.io.Charsets;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.DisconnectMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessageWithKey;
import pku.netlab.hermes.ClusterCommunicator;
import pku.netlab.hermes.parser.MQTTEncoder;
import pku.netlab.hermes.pushservice.IPushService;

import java.util.List;

/**
 * Created by hult on 2/10/17.
 * All methods of this class should be thread safe, since they will be called from different threads.
 */
public abstract class CoreProcessor implements IPushService{
    protected ClusterCommunicator clusterCommunicator;
    protected ISessionStore sessionStore;
    protected IMessageStore messageStore;
    protected IMessageQueue messageQueue;
    protected List<MQTTServer> mqttServers;
    protected String brokerID;
    protected String ipAddress;
    protected LocalMap<String, String> localSessionMap;
    protected Vertx serverVertx;
    protected MQTTEncoder encoder = new MQTTEncoder();


    public CoreProcessor() {
        //this vertx is created to init all BrokerVerticle
    }

    public abstract void onCreatingSession(String clientID, Handler<AsyncResult<Void>> handler);

    public abstract void onSessionCreated(MQTTSession session);

    public abstract void clientLogout(String clientID);

    public abstract void saveMessage(AbstractMessage message, Handler<String> handler) throws Exception;

    public abstract void delMessage(String key, String clientID);

    public abstract void delMessage(String key);

    public abstract String getClientSession(String clientID);

    public abstract void updateClientStatus(String clientID);

    public abstract void handleMsgFromMQ(byte[] byteMsg);

    public abstract void onPubAck(String client, String uniqID);

    public abstract void getPendingMessages(String clientID, Handler<List<PublishMessageWithKey>> handler);

    public void setMessageStore(IMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public void setSessionStore(ISessionStore sessionStore) {
        this.sessionStore = sessionStore;
    }

    public void setMessageQueue(IMessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public void setMqttServers(List<MQTTServer> mqttServers) {
        this.mqttServers = mqttServers;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setBrokerID(String brokerID) {
        this.brokerID = brokerID;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getBrokerID() {
        return brokerID;
    }

    public void setClusterCommunicator(ClusterCommunicator clusterCommunicator) {
        this.clusterCommunicator = clusterCommunicator;
    }

    public void setLocalSessionMap(LocalMap<String, String> localSessionMap) {
        this.localSessionMap = localSessionMap;
    }

    public void setServerVertx(Vertx serverVertx) {
        this.serverVertx = serverVertx;
    }
}
