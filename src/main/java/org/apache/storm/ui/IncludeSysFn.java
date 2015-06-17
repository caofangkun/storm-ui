package org.apache.storm.ui;


import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.util.thread.BaseCallback;


/**
 * IncludeSysFn
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
@ClojureClass(className = "backtype.storm.ui.core#mk-include-sys-fn")
public class IncludeSysFn extends BaseCallback {
  private boolean isIncludeSys;

  public IncludeSysFn(boolean isIncludeSys) {
    this.isIncludeSys = isIncludeSys;
  }

  @Override
  public <T> Object execute(T... args) {
    if (isIncludeSys) {
      return true;
    }

    if (args != null && args.length > 0) {
      Object stream = (Object) args[0];
      return (stream instanceof String) && !Common.systemId((String) stream);
    }

    return false;
  }

}
