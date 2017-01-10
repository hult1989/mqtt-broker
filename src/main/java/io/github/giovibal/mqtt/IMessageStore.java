package io.github.giovibal.mqtt;

import org.dna.mqtt.moquette.proto.messages.MessageIDMessage;

import java.util.*;
import java.util.stream.Collectors;

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
                messageStore = new InMemoryMsgStore();
                break;
        }
        return messageStore;
    };

    void loadStore();

    void dropAllMessages();

    List<MessageIDMessage> getAllMessages();

    void enqueue(MessageIDMessage message);

    //MessageIDMessage deque();

    void removeMessage(int msgID);

    static class InMemoryMsgStore implements IMessageStore {
        LinkedHashMap<Integer, MessageIDMessage> messages = new LinkedHashMap<>();

        @Override
        public void loadStore() {
        }

        @Override
        public void dropAllMessages() {
            messages.clear();
        }

        @Override
        public void enqueue(MessageIDMessage message) {
            messages.put(message.getMessageID(), message);
        }

        @Override
        public void removeMessage(int msgID) {
            messages.remove(msgID);
        }

        @Override
        public List<MessageIDMessage> getAllMessages() {
            List<MessageIDMessage> ret = messages.values().stream().collect(Collectors.toList());
            return ret;
        }
    }
}
