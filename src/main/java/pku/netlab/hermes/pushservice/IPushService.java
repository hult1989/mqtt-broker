package pku.netlab.hermes.pushservice;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

import java.io.UnsupportedEncodingException;

/**
 * Created by hult on 2017/7/23.
 */
public interface IPushService {
    public void push(PushTask.UniPush task, Handler<JsonObject> handler) throws Exception;
    public void pushToMany(PushTask.UniPush task, Handler<JsonObject> handler);
    public void broadcast(PushTask.UniPush task, Handler<JsonObject> handler);
}
