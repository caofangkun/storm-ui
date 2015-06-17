package org.apache.storm.ui.core.api;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class ApiCommon {
  public static final String FORMAT_JSON = "json";
  public static final String FORMAT_XML = "xml";
  public static final String FORMAT_PARAM = "format";
  public static final String ID_PARAM = "id";
  public static final String COMPONENT_PARAM = "component";
  // Default value :all-time| Window duration for metrics in seconds
  public static final String WINDOW = "window";
  // Values 1 or 0. Default value 0
  // Controls including sys stats part of theresponse
  public static final String SYS = "sys";
}
