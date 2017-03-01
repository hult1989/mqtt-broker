package org.dna.mqtt.moquette.proto.messages;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

/**
 * Created by hult on 3/1/17.
 */
public class EBMessageCodec implements MessageCodec<JsonObject, AbstractMessage> {
    @Override
    public void encodeToWire(Buffer buffer, JsonObject object) {

    }

    @Override
    public AbstractMessage decodeFromWire(int pos, Buffer buffer) {
        return null;
    }

    @Override
    public AbstractMessage transform(JsonObject object) {
        return null;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public byte systemCodecID() {
        return 0;
    }
}
