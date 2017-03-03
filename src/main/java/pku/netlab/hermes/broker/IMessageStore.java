package pku.netlab.hermes.broker;

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

    ;

    void loadStore();

    void dropAllMessages(String clientID);

    List<MessageIDMessage> getAllMessages(String clientID);

    void save(MessageIDMessage message, String clientID);

    //MessageIDMessage deque();

    void removeMessage(int msgID);

}
