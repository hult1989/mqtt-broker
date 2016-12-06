package io.github.giovibal.mqtt.pushservice;

import io.github.giovibal.mqtt.QOSUtils;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

import java.io.ObjectInput;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/**
 * Created by kindt on 2016/7/22 0022.
 */
public class PushMessage {
    public static PublishMessage convert(JsonObject jsonObject) throws UnsupportedEncodingException {
        PublishMessage publishMessage = new PublishMessage();

        String uid = jsonObject.getString("uid");
        String payload = jsonObject.getString("payload");
        int msgID = jsonObject.getInteger("msgID");
        byte qos = jsonObject.getInteger("QoS", 0).byteValue();

        publishMessage.setTopicName(uid);
        publishMessage.setPayload(payload);
        publishMessage.setQos(new QOSUtils().toQos(qos));
        publishMessage.setMessageID(msgID);

        return publishMessage;
    }

    public static void main(String[] args) {
    }
}
