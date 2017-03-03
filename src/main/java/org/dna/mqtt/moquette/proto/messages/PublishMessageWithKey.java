package org.dna.mqtt.moquette.proto.messages;

/**
 * Created by hult on 3/3/17.
 */
public class PublishMessageWithKey extends PublishMessage{
    private String globalKey;
    public PublishMessageWithKey(String strPub, String key) throws Exception {
        super(strPub);
        this.globalKey = key;
    }

    public String getGlobalKey() {
        return globalKey;
    }
}
