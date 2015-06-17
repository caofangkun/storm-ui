package org.apache.storm.ui.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.storm.daemon.worker.executor.ExecutorType;
import org.apache.storm.daemon.worker.stats.Pair;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.Thrift;
import org.apache.storm.thrift.Thrift.InternalBoltSpec;
import org.apache.storm.thrift.Thrift.InternalSpoutSpec;
import org.apache.storm.ui.core.Core;
import org.apache.storm.util.CoreUtil;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.TestPlannerBolt;
import backtype.storm.testing.TestPlannerSpout;


/**
 * @author caokun {@link caofangkun@gmail.com}
 * @author zhangxun {@link xunzhang555@gmail.com}
 * 
 */
public class CoreTest {
  private final static Logger LOG = LoggerFactory.getLogger(CoreTest.class);

  private static String localTmpPath = null;

  @BeforeClass
  public static void SetUp() {
    localTmpPath = CoreUtil.localTempPath();
    try {
      FileUtils.forceMkdir(new File(localTmpPath));
    } catch (IOException e) {
      LOG.error("set up failed.", e);
    }
  }

  @Test
  public void testGetFilledStats() {
    List<ExecutorSummary> inputs = new ArrayList<ExecutorSummary>();
    ExecutorSummary summary1 = new ExecutorSummary();
    summary1.set_component_id("summary1");
    summary1.set_stats(null);
    inputs.add(summary1);
    ExecutorSummary summary2 = new ExecutorSummary();
    summary2.set_component_id("summary2");
    summary2.set_stats(null);
    inputs.add(summary2);
    Assert.assertEquals(0, Core.getFilledStats(inputs).size());

    ExecutorStats stats = new ExecutorStats();
    HashMap<String, Long> emitted = new HashMap<String, Long>();
    emitted.put("s1", 100L);
    emitted.put("s2", 200L);
    stats.put_to_emitted("600", emitted);
    HashMap<String, Long> transferred = new HashMap<String, Long>();
    transferred.put("s1", 300L);
    transferred.put("s2", 400L);
    stats.put_to_transferred("600", transferred);
    ExecutorSummary summary3 = new ExecutorSummary();
    summary3.set_component_id("summary1");
    summary3.set_stats(stats);

    inputs.add(summary3);

    List<ExecutorStats> expected = new ArrayList<ExecutorStats>();
    expected.add(stats.deepCopy());
    Assert.assertEquals(expected, Core.getFilledStats(inputs));

  }

  @Test
  public void testComponentType() {
    Map<String, InternalSpoutSpec> spouts =
        new HashMap<String, Thrift.InternalSpoutSpec>(1);
    spouts.put("1",
        Thrift.mkInternalSpoutSpec(new TestPlannerSpout(false), 3, null, null));
    Map<String, InternalBoltSpec> bolts =
        new HashMap<String, Thrift.InternalBoltSpec>(2);
    Map<Object, Object> inputs1 = new HashMap<Object, Object>(1);
    inputs1.put("1", Thrift.InternalGroupingType.NONE);
    bolts.put("2", Thrift.mkInternalBoltSpec(inputs1, new TestPlannerBolt(), 4,
        null, null));
    Map<Object, Object> inputs2 = new HashMap<Object, Object>(1);
    inputs2.put("2", Thrift.InternalGroupingType.NONE);
    bolts.put("3", Thrift.mkInternalBoltSpec(inputs2, new TestPlannerBolt(),
        null, null, null));
    StormTopology topology = Thrift.mkTopology(spouts, bolts);

    Assert.assertEquals(ExecutorType.spout, Core.componentType(topology, "1"));
    Assert.assertEquals(ExecutorType.bolt, Core.componentType(topology, "2"));
    Assert.assertEquals(ExecutorType.bolt, Core.componentType(topology, "3"));
  }

  @Test
  public void testAddPairs() {
    Pair<Object, Object> pairs1 = new Pair<Object, Object>(1, 3);
    Pair<Object, Object> pairs2 = new Pair<Object, Object>(4, 5);
    Pair<Object, Object> result = Core.addPairs(pairs1, pairs2);
    Assert.assertEquals(5, result.getFirst());
    Assert.assertEquals(8, result.getSecond());

  }

  @Test
  public void testWorkerLogLink() {
    Assert.assertEquals("http://localhost:8000/log?file=worker-6701.log",
        Core.workerLogLink("localhost", 6701));

  }

  @Test
  public void testTopologyConf() {
    Map<String, Object> stormConf = new HashMap<String, Object>();
    stormConf.put(Config.STORM_ZOOKEEPER_PORT, 1234);
    stormConf.put(Config.STORM_ZOOKEEPER_ROOT, "/storm/test");

    String resultFile =
        localTmpPath + CoreUtil.filePathSeparator() + "topology-conf.test";
    try {
      OutputStreamWriter out;
      CoreUtil.touch(resultFile);
      out = new OutputStreamWriter(new FileOutputStream(new File(resultFile)));
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);

      dumpGenerator.writeStartObject();
      Core.topologyConf(stormConf, dumpGenerator);
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      String outFile =
          System.getProperty("user.dir")
              + "/src/test/results/topology-conf.json";
      int res = QTestUtils.executeDiffCommand(outFile, resultFile, true);
      Assert.assertEquals(0, res);

    } catch (Exception e) {
      LOG.error("test topology configuration failed", e);
    }
  }

  @Test
  public void testCheckIncludeSys() {
    Assert.assertTrue(Core.checkIncludeSys("true"));
    Assert.assertFalse(Core.checkIncludeSys("false"));
    Assert.assertFalse(Core.checkIncludeSys(null));
    Assert.assertFalse(Core.checkIncludeSys("123"));

  }

  @Test
  public void testExceptionToJson() {
    Exception ex = new IOException("test exception");
    String resultFile =
        localTmpPath + CoreUtil.filePathSeparator()
            + "exception-to-json.test";
    OutputStreamWriter out;
    try {
      CoreUtil.touch(resultFile);
      out = new OutputStreamWriter(new FileOutputStream(new File(resultFile)));
      try {
        Core.exceptionToJson(ex, out);
        String outFile =
            System.getProperty("user.dir")
                + "/src/test/results/exception-to-json.json";
        int res = QTestUtils.executeDiffCommand(outFile, resultFile, true);
        Assert.assertEquals(0, res);
      } catch (TException e) {
        e.printStackTrace();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void CleanUp() {
    if (localTmpPath != null) {
      try {
        CoreUtil.rmr(localTmpPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
