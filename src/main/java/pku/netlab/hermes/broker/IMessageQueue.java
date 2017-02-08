package pku.netlab.hermes.broker;

import io.vertx.core.Handler;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

/**
 * Created by hult on 1/11/17.
 */
public interface IMessageQueue {

    /*
    all these interfaces may be called from different eventloops in different threads, so make them thread safe.
    using eventbus may be a good idea, but be aware of eventbus address conflicts.
    */

    void setMessageHandler(Handler<PublishMessage> handler);

    void enqueue(PublishMessage message);
}
