/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.ui.core.api.topology;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.ClojureClass;
import org.apache.storm.ui.core.Core;
import org.apache.storm.ui.core.api.ApiCommon;


/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
@ClojureClass(className = "backtype.storm.ui.core#topology-page")
public class TopologyPageServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;
  private String topologyId;
  private String window;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    topologyId = request.getParameter(ApiCommon.ID_PARAM);
    if (topologyId == null) {
      throw new IOException("id should not be null!");
    }
    window = request.getParameter(ApiCommon.WINDOW);
    if (window == null) {
      window = "0";
    }
    // "true" or "false"
    String isIncludeSys = request.getParameter(ApiCommon.SYS);
    if (isIncludeSys == null) {
      isIncludeSys = "false";
    }

    response.setContentType("text/javascript");

    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.topologyPage(topologyId, window, Core.checkIncludeSys(isIncludeSys),
          out);
    } catch (Exception e) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
    }
    out.close();
  }
}
