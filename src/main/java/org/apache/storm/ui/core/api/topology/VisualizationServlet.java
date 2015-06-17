package org.apache.storm.ui.core.api.topology;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.thrift.TException;
import org.apache.storm.ui.core.Core;
import org.apache.storm.ui.core.api.ApiCommon;

/**
 * 
 * /api/v1/topology/:id/visualization
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
public class VisualizationServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private String topologyId;
  private String component;
  private String window;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    topologyId = request.getParameter(ApiCommon.ID_PARAM);
    if (topologyId == null) {
      throw new IOException("topology id should not be null!");
    }

    component = request.getParameter(ApiCommon.COMPONENT_PARAM);
    if (component == null) {
      throw new IOException("component id should not be null!");
    }

    window = request.getParameter(ApiCommon.WINDOW);
    if (window == null) {
      window = "All-time";
    }

    String isIncludeSys = request.getParameter(ApiCommon.SYS);
    if (isIncludeSys == null) {
      isIncludeSys = "false";
    }

    response.setContentType("text/javascript");

    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.mkVisualizationData(topologyId, window, isIncludeSys.equals("true"),
          out);
    } catch (TException e) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
    }
    out.close();

  }
}
