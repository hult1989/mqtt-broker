package pku.netlab.hermes;

import io.vertx.core.AbstractVerticle;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import pku.netlab.hermes.broker.CoreProcessor;

/**
 * Created by hult on 2/28/17.
 */
public class ClusterCommunicator extends AbstractVerticle {
    private ZookeeperClusterManager manager;
    private CoreProcessor processor;

    public ClusterCommunicator(ZookeeperClusterManager manager, CoreProcessor processor) {
        this.manager = manager;
        this.processor = processor;
    }

    @Override
    public void start() throws Exception {
        System.out.println("CM start at " + Thread.currentThread().getName());
    }
}
