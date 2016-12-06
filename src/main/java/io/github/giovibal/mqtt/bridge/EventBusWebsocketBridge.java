package io.github.giovibal.mqtt.bridge;

import io.github.giovibal.mqtt.MQTTSession;
import io.github.giovibal.mqtt.MQTTWebSocketWrapper;
import io.github.giovibal.mqtt.WebSocketWrapper;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.streams.Pump;

import java.util.UUID;

/**
 * Created by Giovanni Baleani on 15/07/2015.
 */
public class EventBusWebsocketBridge {
    private static final String BR_HEADER = "bridged";

    private WebSocketBase webSocket;
    private EventBus eventBus;
    private String eventBusAddress;
    private String tenant;
    private DeliveryOptions deliveryOpt;
    private MessageConsumer<Buffer> consumer;
    private MessageProducer<Buffer> producer;
//    private MqttPump fromRemoteTcpToLocalBus;
    private Pump fromRemoteTcpToLocalBus;
    private WebSocketWrapper netSocketWrapper;
    private String bridgeUUID;

    public EventBusWebsocketBridge(WebSocketBase webSocket, EventBus eventBus, String eventBusAddress) {
        this.eventBus = eventBus;
        this.webSocket = webSocket;
        this.eventBusAddress = eventBusAddress;
        this.bridgeUUID = UUID.randomUUID().toString();
    }

    public void init() {
        deliveryOpt = new DeliveryOptions().addHeader(BR_HEADER, bridgeUUID);
        if(tenant!=null) {
            deliveryOpt.addHeader(MQTTSession.TENANT_HEADER, tenant);
        }
        consumer = eventBus.consumer(eventBusAddress);
        producer = eventBus.publisher(eventBusAddress, deliveryOpt);
        fromRemoteTcpToLocalBus = new MqttPump(webSocket, producer);
//        fromRemoteTcpToLocalBus = Pump.pump(webSocket, producer);
        netSocketWrapper = new MQTTWebSocketWrapper(webSocket);
    }

    public void start() {
        init();
        webSocket.pause();
        consumer.pause();
        // from remote tcp to local bus
        fromRemoteTcpToLocalBus.start();

        // from local bus to remote tcp
        consumer.handler(bufferMessage -> {
            boolean isBridged = bufferMessage.headers() != null
                    && bufferMessage.headers().contains(BR_HEADER)
                    && bufferMessage.headers().get(BR_HEADER).equals(bridgeUUID)
                    ;
            if (!isBridged) {
                boolean tenantMatch = tenantMatch(bufferMessage);
                if(tenantMatch) {
                    netSocketWrapper.sendMessageToClient(bufferMessage.body());
                }
            }
        });
        consumer.resume();
        webSocket.resume();
    }

    // TODO: this method is equal to MQTTSession.isTenantSession, need refactoring
    private boolean isTenantSession() {
        boolean isTenantSession = tenant!=null && tenant.trim().length()>0;
        return isTenantSession;
    }
    // TODO: this method is equal to MQTTSession.tenantMatch, need refactoring
    private boolean tenantMatch(Message<Buffer> message) {
        boolean isTenantSession = isTenantSession();
        boolean tenantMatch;
        if(isTenantSession) {
            boolean containsTenantHeader = message.headers().contains(MQTTSession.TENANT_HEADER);
            if (containsTenantHeader) {
                String tenantHeaderValue = message.headers().get(MQTTSession.TENANT_HEADER);
                tenantMatch =
                        tenant.equals(tenantHeaderValue)
                                || "".equals(tenantHeaderValue)
                ;
            } else {
                // if message doesn't contains header is not for a tenant-session
                tenantMatch = false;
            }
        } else {
            // if this is not a tenant-session, receive all messages from all tenants
            tenantMatch = true;
        }
        return tenantMatch;
    }

    String getBridgeUUID() {
        return bridgeUUID;
    }

    public void stop() {
        // from remote tcp to local bus
        fromRemoteTcpToLocalBus.stop();
        // from local bus to remote tcp
        netSocketWrapper.stop();// stop write to remote tcp socket
        consumer.handler(null);// stop read from bus
    }



    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getTenant() {
        return tenant;
    }
}
