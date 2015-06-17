package org.apache.storm.ui.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.cluster.StormZkClusterState;
import org.apache.storm.ui.jetty.JettyEmbeddedServer;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.NetWorkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.NodeInfo;
import backtype.storm.utils.Utils;


/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.ui.core#main")
public class UIServer {
  private static final Logger LOG = LoggerFactory.getLogger(UIServer.class);

  private InetSocketAddress httpAddress = null;
  private JettyEmbeddedServer httpServer;
  @SuppressWarnings("rawtypes")
  private static Map conf = Utils.readStormConfig();

  @ClojureClass(className = "backtype.storm.ui.core#start-server!")
  public void startServer() throws IOException {
    httpAddress = getHttpServerAddress();
    String infoHost = httpAddress.getHostName();
    int infoPort = httpAddress.getPort();
    httpServer =
        new JettyEmbeddedServer("storm-ui", infoHost, infoPort, infoPort == 0,
            conf);
    httpServer.setAttribute("ui.port", infoPort);
    try {
      StormClusterState stormClusterState = new StormZkClusterState(conf);
      Set<Long> ports = new HashSet<Long>();
      ports.add(Long.valueOf(infoPort));
      stormClusterState.registerUIServerInfo(new NodeInfo(infoHost, ports));
    } catch (Exception e) {
      throw new IOException(e);
    }
    httpServer.start();
    LOG.info(httpServer.toString());
  }

  @SuppressWarnings("unchecked")
  private static InetSocketAddress getHttpServerAddress() throws IOException {
    String uiHost = CoreUtil.localHostname();
    int uiPort = CoreUtil.parseInt(conf.get(Config.UI_PORT), 8081);

    uiPort = NetWorkUtils.assignServerPort(uiPort);
    conf.put(Config.UI_PORT, uiPort);

    return NetWorkUtils.createSocketAddr(uiHost + ":" + String.valueOf(uiPort));
  }

  public static void main(String[] args) {
    UIServer server = new UIServer();
    try {
      server.startServer();
    } catch (IOException e) {
      LOG.error(CoreUtil.stringifyError(e));
      e.printStackTrace();
    }
  }
}
