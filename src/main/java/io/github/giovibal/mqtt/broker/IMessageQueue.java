package io.github.giovibal.mqtt.broker;

/**
 * Created by hult on 1/11/17.
 */
public interface IMessageQueue {
    /* all these interfaces may be called from different eventloops in different threads, so make them thread safe.
    using eventbus may be a good idea, but be aware of eventbus address conflicts.
    we can use only one kafka producer verticle instead of multiple kafka clients.
     */
}
