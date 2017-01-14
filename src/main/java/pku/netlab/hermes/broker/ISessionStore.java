package pku.netlab.hermes.broker;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Created by hult on 1/7/17.
 */
public interface ISessionStore {

    void addClient(String brokerID, String clientID, Handler<AsyncResult<Void>> handler);

    void removeClient(String brokerID, String clientID, Handler<AsyncResult<Void>> handler);

    void brokerOfClient(String clientID, Handler<AsyncResult<String>> handler);

    void clearBrokerSession(String brokerID, Handler<AsyncResult<Void>> handler);

}
