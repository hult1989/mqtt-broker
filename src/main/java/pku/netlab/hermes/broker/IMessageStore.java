package pku.netlab.hermes.broker;

import io.vertx.core.Handler;
import org.dna.mqtt.moquette.proto.messages.MessageIDMessage;

import java.util.List;

/**
 * Created by hult on 1/7/17.
 */
public interface IMessageStore {
    static IMessageStore getStore(String type) {
        //load store from DBClient without having to hold vertx instance
        IMessageStore messageStore = null;
        switch (type) {
            case "MongoDB":
                break;
            case "HBase":
                break;
            default:
                break;
        }
        return messageStore;
    }

    void batchGet(List<String> messageIDs, Handler<List<String>> handler);

    void batchRem(List<String> messageIDs, Handler<Boolean> handler);

    void rem(String id, Handler<Boolean> handler);

    void get(String id, Handler<String> handler);

    void save(String message, Handler<String> keyHandler);

    void batchSave(List<String> messages, Handler<List<String>> handler);
}
