package pku.netlab.hermes.broker;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import org.dna.mqtt.moquette.proto.messages.PublishMessageWithKey;

import java.util.List;

/**
 * Created by hult on 1/7/17.
 */
public interface ISessionStore {

    void addClient(String brokerID, String clientID, Handler<AsyncResult<Void>> handler);

    void removeClient(String brokerID, String clientID, Handler<AsyncResult<Void>> handler);

    void brokerOfClient(String clientID, Handler<AsyncResult<String>> handler);

    void clearBrokerSession(String brokerID, Handler<AsyncResult<Void>> handler);

    void getAllMembers(String brokerID, Handler<AsyncResult<JsonArray>> handler);

    void pendingMessages(String clientID, Handler<List<PublishMessageWithKey>> handler);

    void removePendingMessage(String key, String clientID);

    void removeMessage(String key);
}
