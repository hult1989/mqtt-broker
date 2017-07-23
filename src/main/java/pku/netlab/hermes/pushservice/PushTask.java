package pku.netlab.hermes.pushservice;


import java.util.List;

/**
 * Created by hult on 2017/7/23.
 */
public class PushTask {
    public final String message;
    public final String uniqueMsgID;

    public PushTask(String message, String uniqID) {
        this.message = message;
        this.uniqueMsgID = uniqID;
    }

    public static class MultiPush extends PushTask {
        List<String> targets;

        public MultiPush(String message, String uniqID, List<String> targets) {
            super(message, uniqID);
            this.targets = targets;
        }
    }
    public static class UniPush extends PushTask {
        public final String target;

        public UniPush(String message, String uniqID, String target) {
            super(message, uniqID);
            this.target = target;
        }
    }
}
