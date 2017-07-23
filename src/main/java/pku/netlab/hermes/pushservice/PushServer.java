package pku.netlab.hermes.pushservice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;


/**
 * Created by kindt on 2016/7/21 0021.
 */
public class PushServer extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(PushServer.class);

    private LocalMap localSessionMap;
    private Router router;
    private HttpServer httpServer;
    private IPushService pushService;

    public PushServer (IPushService pushService) {
        this.pushService = pushService;
        this.router = Router.router(vertx);

        router.get("/user").handler(rc -> {
            logger.debug("get /user");
            this.getOnlineClients(usersArray -> {
                rc.response().write(usersArray.toString()).end();
            });
        });
        router.post("/user/push/:client_id").handler(rc -> {
            String clientID = rc.request().getParam("client_id");
            String uniqID = rc.request().getParam("msg_id");
            String message = rc.getBodyAsString();
            message = "some message";
            PushTask.UniPush task = new PushTask.UniPush(message, uniqID, clientID);

            try {
                this.push(task, ack -> {
                    rc.response().setChunked(true);
                    rc.response().write(ack.toString()).end();
                });
            } catch (Exception e) {
                e.printStackTrace();
                rc.response().setChunked(true);
                rc.response().write(new JsonObject().put("ack", -1).toString()).end();
            }
        });
        router.get("/user/:clientID").handler(rc -> {
            String clientID = rc.request().getParam("clientID");
            try {
                this.ping(clientID, ack -> {
                    rc.response().write(ack.toString()).end();
                });
            } catch (Exception e) {
                rc.response().write(new JsonObject().put("ack", -1).toString()).end();
            }
        });
        router.post("/user/broadcast").handler(rc -> {
            this.broadcast(rc.getBodyAsString());
            this.getOnlineClients(usersArray -> {
                rc.response().write(usersArray.toString()).end();
            });
        });
    }


    @Override
    public void start() throws Exception {
        this.localSessionMap = vertx.sharedData().getLocalMap("LOCAL_SESSION_MAP");
        this.httpServer = vertx.createHttpServer();
        this.httpServer.requestHandler(this.router::accept).listen(config().getInteger("port"));
        logger.info("push server start");
    }

    private void push(PushTask.UniPush task, Handler<JsonObject> handler) throws Exception {
        pushService.push(task, handler);
    }

    private void broadcast(String message) {
        vertx.eventBus().publish("BROADCAST", message);
    }

    private void getOnlineClients(Handler<JsonArray> handler) {
        JsonArray array = new JsonArray();
        localSessionMap.forEach((k, v)-> {
            array.add(new JsonObject().put((String) k, (String) v));
        });
        handler.handle(array);
    }

    private void ping(String clientID, Handler<JsonObject> handler) throws Exception {
        PushTask.UniPush task = new PushTask.UniPush("", "-1", clientID);
        this.push(task,  handler);
    }
}
