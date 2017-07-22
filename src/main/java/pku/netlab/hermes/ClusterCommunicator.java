package pku.netlab.hermes;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import pku.netlab.hermes.broker.CoreProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hult on 2/28/17.
 */
public class ClusterCommunicator extends AbstractVerticle {
    private ZookeeperClusterManager manager;
    private CoreProcessor processor;
    private final Logger logger = LoggerFactory.getLogger(ClusterCommunicator.class);
    private Map<String, String> map;

    public ClusterCommunicator(ZookeeperClusterManager manager, CoreProcessor processor) {
        this.manager = manager;
        this.processor = processor;
        this.manager.nodeListener(new NodeListener() {
            @Override
            public void nodeAdded(String nodeID) {
                System.out.println(String.format("node %s joined!", nodeID));
            }

            @Override
            public void nodeLeft(String nodeID) {
                System.out.println(String.format("node %s left!", nodeID));
                standbySwitch(nodeID);
            }
        });
    }

    private void standbySwitch(String nodeID) {
        if (!nodeID.equals("standby")) {
            return;
        }
        logger.info("hot standby switching...");
        String IDofCrashedBroker = map.remove(nodeID);
        processor.updateBrokerID(IDofCrashedBroker);
        map.put(manager.getNodeID(), IDofCrashedBroker);
        map.put(processor.getBrokerID(), processor.getHost());
        processor.start();
    }

    @Override
    public void start() throws Exception {
        logger.info(String.format("%s assigned with brokerID %s", manager.getNodeID(), processor.getBrokerID()));
        logger.info("working nodes: " + String.valueOf(manager.getNodes()));
        this.map = manager.getSyncMap("brokers");
        map.put(manager.getNodeID(), processor.getBrokerID());
        map.put(processor.getBrokerID(), processor.getHost());
    }

    public String getBrokers() {
        Map<String, String> ret = new HashMap<>();
        for (String node : manager.getNodes()) {
            String id = map.get(node);
            String ip = map.get(id);
            ret.put(ip, id + ": " + node);
        }
        return ret.toString();
    }
}
