package pku.netlab.hermes.broker;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.stream.Stream;

/**
 * Created by hult on 2017/7/23.
 */
public class Utils {
    static public String getIpAddress(String networkID) {
        Stream<String> ipStream = Stream.empty();
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface ni = enumeration.nextElement();
                ipStream = Stream.concat(ipStream, ni.getInterfaceAddresses().stream().map(ia -> ia.getAddress().getHostAddress()));
            }
        } catch (SocketException e) {
            System.out.println(e.fillInStackTrace());
            System.exit(0);
        }
        String ret = ipStream.filter(ip -> ip.startsWith(networkID)).findFirst().get();
        if (ret == null) {
            System.out.println("failed to find ip address starts with " + networkID);
            System.exit(0);
        }
        return ret;
    }
}
