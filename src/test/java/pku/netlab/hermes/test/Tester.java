package pku.netlab.hermes.test;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;

/**
 * Created by giovanni on 08/04/2014.
 */
public class Tester {
    static final String serverURL = "tcp://localhost:1883";
    static final String serverURLSubscribers = serverURL;
    static final String serverURLPublishers = serverURL;


    static boolean logEnabled=false;

    public static void main(String[] args) throws Exception {

//        stats("Qos Tests");
//        test2(30, 100, 0, 0);
//        test2(30, 100, 1, 0);
//        test2(30, 100, 2, 0);

        stats("Num Clients / Num Messages Tests");
        for(int i=0; i<5; i++) {
            test2(200, 5, 0, 1);
            Thread.sleep(40*1000);
        }
//        test2(100, 30, 0, 1);
//        test2(3, 1000, 0, 1);
//        test2(5, 1000, 0, 10);
//        test2(5, 1000, 0, 1);
//        test2(5, 20000, 0, 0);
//        test2(100, 500, 0, 0);
//        test2(10, 10000, 0, 0);

//        test2(30, 200, 0, 0);
//        test2(30, 500, 0, 0);
//        test2(30, 800, 0, 0);
//        test2(30, 1000, 0, 0);
//        test2(30, 100, 1, 1);
//        test2(30, 100, 2, 1);

//        test2(2, 10000, 2);// 2 client che pubblicano 10000 messaggi ciascuno con qos:2 (4368 millis. arrivati 20000 messaggi)
//        test2(10, 2000, 2);// 10 client che pubblicano 2000 messaggi ciascuno con qos:2 (11218 millis. arrivati 20000 messaggi)
//        test2(20, 1000, 2);// 20 client che pubblicano 1000 messaggi ciascuno con qos:2 (21217 millis. arrivati 20000 messaggi)
//        test2(40, 500, 2);// 40 client che pubblicano 500 messaggi ciascuno con qos:2 (40742 millis. arrivati 20000 messaggi)
//        test2(40, 50000, 0, 1);// 40 client che pubblicano 500 messaggi ciascuno con qos:0 (4926 millis. arrivati circa 2320 messaggi)
//        test2(1000, 2, 2);// 10000 client che pubblicano 2 messaggi ciascuno con qos:2 (4368 millis. arrivati 20000 messaggi)

//        test2(10, 30000, 2);// 10 client che pubblicano 30000 messaggi ciascuno con qos:2 (??? millis. arrivati 300000 messaggi)
        // connectionLost Timed out waiting for a response from the server

//        test2(10, 30000, 0);// 10 client che pubblicano 30000 messaggi ciascuno con qos:0 (59321 millis. arrivati 300000 messaggi)
//        test2(10, 10000, 0);// 10 client che pubblicano 10000 messaggi ciascuno con qos:0 (19737 millis. arrivati 100000 messaggi)
//                                                                           Con Hive 2.0.2 (21535 millis. arrivati in media 93000 messaggi)
//        test2(10, 10000, 2);// 10 client che pubblicano 60000 messaggi ciascuno con qos:2 (36162 millis.)
//                                                                           Con Hive 2.0.2 (45347 millis. ma arrivati 97446 messaggi per tutti i client - NullPointer sul server)
//        test2(10, 10000, 1);// 10 client che pubblicano 60000 messaggi ciascuno con qos:1 (22609 millis.)
//                                                                           Con Hive 2.0.2 (32536 millis. ma arrivati 97435 messaggi per 9 client e 97433 per il primo)
//        test3(100);
//        test4(10, 2);
//        test4(100, 20);
//        logEnabled=false;
//        test4(1000, 200);
    }

    private static void log(String msg) {
        if(logEnabled) {
            System.out.println(msg);
        }
    }
    private static void stats(String msg) {
        System.out.println(msg);
    }


    private static String generateRandomTopic(String pattern) {
        String rnd = UUID.randomUUID().toString();
        String topic = pattern;
        while(topic.contains("RND"))
            topic = topic.replaceFirst("RND", rnd);
        return topic;
    }

    public static void test2(int numClients, int numMessagesToPublishPerClient, int qos, long sleepMilliSeconds) throws Exception {
        stats("");
        stats("------------------------------------------------------------------");
        stats("Test clients: "+ numClients +", num msg: "+ numMessagesToPublishPerClient +", qos: "+ qos +" sleep: "+sleepMilliSeconds +" millis.");
        stats("------------------------------------------------------------------");
//        String topic = "test/untopic/a";
        String topic = generateRandomTopic("test/RND/RND/a");
        String topicFilter = "test/+/+/a";

        long t1,t2,t3;
        t1=System.currentTimeMillis();

        Tester cSubs = new Tester(numClients, "SUBS", serverURLSubscribers);
        cSubs.connect();
        cSubs.subscribe(topicFilter);

        Tester cPubs = new Tester(numClients, "PUBS", serverURLPublishers);
        cPubs.connect();

        boolean retain = true;
        cPubs.publish(numMessagesToPublishPerClient, topic, qos, retain);
        cPubs.disconnect();

        if(sleepMilliSeconds>0) {
            log("Sleep for " + sleepMilliSeconds + " millis ...");
            Thread.sleep(sleepMilliSeconds);
        }

//        cSubs.unsubcribe(topic);
//        cSubs.disconnect();

        cPubs.publishStats();
        cSubs.subscribeStats();

        t2=System.currentTimeMillis();
        t3=t2-t1;


        stats("Time elapsed: " + t3 + " millis.");
        stats("Messages sent: " + cPubs.getMessaggiSpeditiInMedia() + " messages.");
        stats("Messages received: " + cSubs.getMessaggiArrivatiInMedia() + " messages.");
        float seconds = (t3/1000);
        if(seconds > 0) {
            float throughputPub = cPubs.getMessaggiSpeditiInMedia() / (seconds);
            float throughputSub = cSubs.getMessaggiArrivatiInMedia() / (seconds);
            float throughput = throughputPub + throughputSub;
            stats("Throughput Published: " + throughputPub + " msg/sec.");
            stats("Throughput Received: " + throughputSub + " msg/sec.");
            stats("Throughput Total: " + throughput + " msg/sec.");
        }
        stats("------------------------------------------------------------------");
    }



    private List<IMqttClient> clients = new ArrayList<>();
    private List<MQTTClientHandler> clientHandlers = new ArrayList<>();

    public Tester(int numClients, String clientIDPrefix, String serverURL) throws MqttException {
        for(int i=1; i<=numClients; i++) {
            String clientID = clientIDPrefix+"_" + i;

            MqttClient client = new MqttClient(serverURL, clientID, new MemoryPersistence());
            MQTTClientHandler h = new MQTTClientHandler(clientID);
            client.setCallback(h);

            clients.add(client);
            clientHandlers.add(h);
        }
    }
    public void connect() throws MqttException {
        log("connect ...");
        for(IMqttClient client : clients) {
            MqttConnectOptions o = new MqttConnectOptions();
//            if(this.serverURL.startsWith("ssl")) {
//                try {
//                        SSLSocketFactory sslSocketFactory = SslUtil.getSocketFactory(
//                                "C:\\Sviluppo\\Certificati-SSL\\CA\\rootCA.pem",
////                                "C:\\Sviluppo\\Certificati-SSL\\device1\\device1_CA1.crt",
//                                "C:\\Sviluppo\\Certificati-SSL\\device1\\device1.crt",
//                                "C:\\Sviluppo\\Certificati-SSL\\device1\\device1.key",
//                                "");
//                        o.setSocketFactory(sslSocketFactory);
//                } catch(Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            o.setCleanSession(true);
//            try {
//                o.setWill("$SYS/config", new String("{\"retain\":false}").getBytes("UTF-8"), 0, false);
//            } catch (Throwable e) { e.printStackTrace(); }
            client.connect(o);
        }
    }

    public void disconnect() throws MqttException {
        log("disconnet ...");
        for(IMqttClient client : clients) {
            client.disconnect();
        }
    }

    public void subscribe(String topic) throws MqttException {
        log("subscribe topic: " + topic + " ...");
        for (IMqttClient client : clients) {
            client.subscribe(topic, 2);
        }
    }
    public void unsubcribe(String topic) throws MqttException {
        log("unsubscribe topic: " + topic + " ...");
        for (IMqttClient client : clients) {
            client.unsubscribe(topic);
        }
    }

    public void publish(String topic) throws Exception {
        log("publih ...");
        MqttMessage m;
        for(IMqttClient client : clients) {

            m = new MqttMessage();
            m.setQos(2);
            m.setRetained(true);
            m.setPayload("prova qos=2 retained=true".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=2 retained=true");
            client.publish(topic, m);

            m = new MqttMessage();
            m.setQos(1);
            m.setRetained(true);
            m.setPayload("prova qos=1 retained=true".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=1 retained=true");
            client.publish(topic, m);

            m = new MqttMessage();
            m.setQos(0);
            m.setRetained(true);
            m.setPayload("prova qos=0 retained=true".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=0 retained=true");
            client.publish(topic, m);

            m = new MqttMessage();
            m.setQos(2);
            m.setRetained(false);
            m.setPayload("prova qos=2 retained=false".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=2 retained=false");
            client.publish(topic, m);

            m = new MqttMessage();
            m.setQos(1);
            m.setRetained(false);
            m.setPayload("prova qos=1 retained=false".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=1 retained=false");
            client.publish(topic, m);

            m = new MqttMessage();
            m.setQos(0);
            m.setRetained(false);
            m.setPayload("prova qos=0 retained=false".getBytes("UTF-8"));
            log(client.getClientId() + " publish >> sending qos=0 retained=false");
            client.publish(topic, m);
        }
    }
    public void publish(int numMessages, String topic, int qos, boolean retained) throws Exception {
        log("publih ...");
        MqttMessage m;
        for(IMqttClient client : clients) {
            for(int i=0; i<numMessages; i++) {
                String msg = "msg "+i+" qos="+qos+" retained="+ retained;
                m = new MqttMessage();
                m.setQos(qos);
                m.setRetained(retained);
                m.setPayload(msg.getBytes("UTF-8"));
                log(client.getClientId() + " publish >> sending qos=" + qos + " retained=" + retained);
                client.publish(topic, m);
            }
        }
    }

    public void stats() {
        log("-------------------------------S-T-A-T-S-------------------------------------------");
        for(MQTTClientHandler h : clientHandlers) {
            log("Client: " + h.clientID + " messaggi arrivati: " + h.messaggiArrivati + " messaggi spediti: " + h.messaggiSpediti);
        }
        log("-------------------------------S-T-A-T-S-------------------------------------------");
    }
    public void publishStats() {
        log("-------------------------------S-T-A-T-S-------------------------------------------");
        for(MQTTClientHandler h : clientHandlers) {
            log("Client: " + h.clientID + " messaggi spediti: " + h.messaggiSpediti);
        }
        log("-------------------------------S-T-A-T-S-------------------------------------------");
    }
    public void subscribeStats() {
        log("-------------------------------S-T-A-T-S-------------------------------------------");
        for(MQTTClientHandler h : clientHandlers) {
            log("Client: " + h.clientID + " messaggi arrivati: " + h.messaggiArrivati);
        }
        log("-------------------------------S-T-A-T-S-------------------------------------------");
    }
    public Map<String, Integer> getMessaggiArrivatiPerClient() {
        Map<String, Integer> ret = new HashMap<>();
        for(MQTTClientHandler h : clientHandlers) {
            ret.put(h.clientID,h.messaggiArrivati);
        }
        return ret;
    }
    public int getMessaggiArrivatiInMedia() {
        int sum=0;
        for(MQTTClientHandler h : clientHandlers) {
            sum += h.messaggiArrivati;
        }
        return sum;
    }
    public int getMessaggiSpeditiInMedia() {
        int sum=0;
        for(MQTTClientHandler h : clientHandlers) {
            sum += h.messaggiSpediti;
        }
        return sum;
    }

    static class MQTTClientHandler implements MqttCallback {

        String clientID;
        int messaggiArrivati;
        int messaggiSpediti;

        MQTTClientHandler(String clientID) {
            this.clientID = clientID;
            this.messaggiArrivati = 0;
            this.messaggiSpediti = 0;
        }

        @Override
        public void connectionLost(Throwable throwable) {
            log(clientID + " connectionLost " + throwable.getMessage());
        }

        @Override
        public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
            messaggiArrivati++;
            log(clientID + " messageArrived <== " + topic + " real qos: " + mqttMessage.getQos() + " ==> " + new String(mqttMessage.getPayload(), "UTF-8") + " " + messaggiArrivati);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            messaggiSpediti++;
            log(clientID + " deliveryComplete ==> " + iMqttDeliveryToken.getMessageId() + " " + iMqttDeliveryToken.getClient().getClientId());
        }

    }


}
