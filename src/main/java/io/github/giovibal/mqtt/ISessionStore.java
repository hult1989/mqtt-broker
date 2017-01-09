package io.github.giovibal.mqtt;

import java.util.HashMap;

/**
 * Created by hult on 1/7/17.
 */
public interface ISessionStore {
    void initStore();

    void addSession(String clientID, MQTTSocket socket);

    MQTTSocket sessionForClient(String clientID);

    static ISessionStore getSessionStore(String type) {
        ISessionStore sessionStore = null;
        switch (type) {
            /*
            case "Redis":
                sessionStore = new RedisSessionStore();
                break;
                */
            default:
                sessionStore = new DBSessionStore();
                break;
        }
        return sessionStore;
    }

    static class DBSessionStore implements ISessionStore{
        HashMap<String, MQTTSocket> sessionMap;
        @Override
        public void initStore() {
            sessionMap = new HashMap<>();
        }

        @Override
        public MQTTSocket sessionForClient(String clientID) {
            return sessionMap.get(clientID);
        }

        @Override
        public void addSession(String clientID, MQTTSocket socket) {
            sessionMap.put(clientID, socket);
        }
    }
}
