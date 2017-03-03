package pku.netlab.hermes.broker.Impl;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.dna.mqtt.moquette.proto.messages.MessageIDMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import pku.netlab.hermes.broker.IMessageStore;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by hult on 3/1/17.
 */
public class HBaseStore extends AbstractVerticle implements IMessageStore {
    private JsonObject config;
    private Connection connection;
    private final String ARCHIVE_TABLE_NAME = "archive";

    @Override
    public void start() throws Exception {
        PublishMessage message = new PublishMessage();
        message.setPayload("hello");
        message.setTopicName("ce");
        long start = System.currentTimeMillis();
        System.out.println(start);
        List<Future> futureList = new ArrayList<>(1000);


        for (int i = 0; i < 1000; i += 1) {
            final int cnt = i;
            Future future = Future.future();
            futureList.add(future);
            vertx.executeBlocking(f -> {
                message.setMessageID(cnt);
                this.save(message, String.format("%08d", cnt));
                f.complete();
            }, false, res->{
                futureList.get(cnt).complete();
            });
        }
        System.out.println(futureList.size());
        CompositeFuture.all(futureList).setHandler(res-> {
            if (res.succeeded()) {
                long now = System.currentTimeMillis();
                System.out.println(now - start);
                System.out.println(now);
            }
        });
    }

    public HBaseStore(JsonObject conf) {
        this.config = conf;
    }

    @Override
    public void loadStore() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", config.getString("quorum"));
        try {
            this.connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }

    }

    @Override
    public void dropAllMessages(String clientID) {

    }

    @Override
    public List<MessageIDMessage> getAllMessages(String clientID) {
        List<MessageIDMessage> messages = new LinkedList<>();
        try {
            Table table = connection.getTable(TableName.valueOf(ARCHIVE_TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return messages;
    }

    @Override
    public void save(MessageIDMessage message, String clientID) {
        try {
            Table table = connection.getTable(TableName.valueOf(ARCHIVE_TABLE_NAME));
            ByteBuffer buf = ByteBuffer.allocate(16);
            Long now = System.nanoTime();
            buf.put(Bytes.toBytes(clientID)).putLong(now);
            Put put = new Put(buf.array());
            put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("client"), Bytes.toBytes(clientID));
            put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("timestamp"), Bytes.toBytes(now));
            put.addColumn(Bytes.toBytes("msg"), Bytes.toBytes("body"), Bytes.toBytes(message.toString()));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeMessage(int msgID) {

    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        JsonObject obj = new JsonObject().put("quorum", "server");
        HBaseStore store = new HBaseStore(obj);
        store.loadStore();
        Vertx.vertx().deployVerticle(store, new DeploymentOptions().setWorker(true).setWorkerPoolSize(20));
        /*
        System.out.println(System.currentTimeMillis());
        PublishMessage message = new PublishMessage();
        message.setPayload("hello");
        message.setTopicName("ce");
        long start = System.currentTimeMillis();
        System.out.println(start);
        for (int i = 0; i < 1000; i += 1) {
            message.setMessageID(i);
            store.save(message, "hult");
        }
        long end = System.currentTimeMillis();
        System.out.println(end);
        System.out.println(end - start);
        */
    }
}
