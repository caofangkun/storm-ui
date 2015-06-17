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
package org.apache.storm.ui.core;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.worker.executor.ExecutorType;
import org.apache.storm.daemon.worker.stats.Pair;
import org.apache.storm.daemon.worker.stats.StatsFields;
import org.apache.storm.guava.collect.Lists;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.Thrift;
import org.apache.storm.ui.IncludeSysFn;
import org.apache.storm.ui.helpers.Helpers;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.version.VersionInfo;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.mortbay.log.Log;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.SpoutStats;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.NimbusClient;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.ui.core")
public class Core {
	private static int maxErrortime = 1800;
	private static int maxErrornum = 200;
	public static final String SPOUT_STR = "spout";
	public static final String BOLT_STR = "bolt";

	@ClojureClass(className = "backtype.storm.ui.core#*STORM-CONF*")
	@SuppressWarnings("rawtypes")
	private static final Map STORM_CONF = Helpers.readStormConfig();

	private static final String SYM_REGEX = "(?![A-Za-z_\\-:\\.]).";

	private static final Pattern REFIND_P = Pattern.compile("^[A-Za-z]");

	/**
	 * Get nimbus client
	 * 
	 * @return NimbusClient
	 */
	@ClojureClass(className = "backtype.storm.ui.core#main-routes#with-nimbus")
	private static NimbusClient withNimbus() {
		return NimbusClient.getConfiguredClient(STORM_CONF);
	}

	/**
	 * getFilledStats
	 * 
	 * @param summs
	 * @return List of not null ExecutorStats
	 */
	@ClojureClass(className = "backtype.storm.ui.core#get-filled-stats")
	public static List<ExecutorStats> getFilledStats(List<ExecutorSummary> summs) {
		List<ExecutorStats> res = new ArrayList<ExecutorStats>();
		for (ExecutorSummary es : summs) {
			ExecutorStats executorStats = es.get_stats();
			if (executorStats != null) {
				res.add(executorStats);
			}
		}
		return res;
	}

	/**
	 * Get Storm Version
	 * 
	 * @return storm version string
	 */
	@ClojureClass(className = "backtype.storm.ui.core#read-storm-version")
	public static String readStormVersion() {
		return VersionInfo.getVersion() + "-r" + VersionInfo.getRevision();
	}

	/**
	 * Get the component type
	 * 
	 * @param topology
	 *            topology
	 * @param id
	 *            component id
	 * @return Returns the component type (either :bolt or :spout) for a given
	 *         topology and component id. Returns nil if not found.
	 */
	@ClojureClass(className = "backtype.storm.ui.core#component-type")
	public static ExecutorType componentType(StormTopology topology, String id) {
		Map<String, Bolt> bolts = topology.get_bolts();
		Map<String, SpoutSpec> spouts = topology.get_spouts();
		ExecutorType type = null;
		if (bolts.containsKey(id)) {
			type = ExecutorType.bolt;
		} else if (spouts.containsKey(id)) {
			type = ExecutorType.spout;
		}
		return type;
	}

	/**
	 * executorSummaryType
	 * 
	 * @param topology
	 * @param s
	 * @return executor type; ExecutorType.spout or ExecutorType.bolt
	 */
	@ClojureClass(className = "backtype.storm.ui.core#executor-summary-type")
	public static ExecutorType executorSummaryType(StormTopology topology,
			ExecutorSummary s) {
		return componentType(topology, s.get_component_id());
	}

	/**
	 * addPairs
	 * 
	 * @param pairs1
	 * @param pairs2
	 * @return Pair
	 */
	@ClojureClass(className = "backtype.storm.ui.core#add-pairs")
	public static Pair<Object, Object> addPairs(Pair<Object, Object> pairs1,
			Pair<Object, Object> pairs2) {

		if (null == pairs1 && null == pairs2) {
			return new Pair<Object, Object>(0, 0);
		} else if (null == pairs1) {
			return pairs2;
		} else if (null == pairs2) {
			return pairs1;
		} else {
			return new Pair<Object, Object>(CoreUtil.add(pairs1.getFirst(),
					pairs2.getFirst()), CoreUtil.add(pairs1.getSecond(),
					pairs2.getSecond()));
		}
	}

	/**
	 * expandAverages
	 * 
	 * @param avg
	 * @param counts
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#expand-averages")
	public static <K1, K2, M1, M2> Map<K1, Map<K2, Pair<Object, Object>>> expandAverages(
			Map<K1, Map<K2, M1>> avg, Map<K1, Map<K2, M2>> counts) {
		Map<K1, Map<K2, Pair<Object, Object>>> ret = new HashMap<K1, Map<K2, Pair<Object, Object>>>();
		for (Map.Entry<K1, Map<K2, M2>> entry : counts.entrySet()) {
			K1 slice = entry.getKey();
			Map<K2, M2> streams = entry.getValue();
			Map<K2, Pair<Object, Object>> tmpValue = new HashMap<K2, Pair<Object, Object>>();
			for (Map.Entry<K2, M2> se : streams.entrySet()) {
				M2 count = se.getValue();
				K2 stream = se.getKey();
				tmpValue.put(
						se.getKey(),
						new Pair<Object, Object>(CoreUtil.multiply(count, avg
								.get(slice).get(stream)), count));
			}
			ret.put(slice, tmpValue);
		}

		return ret;
	}

	/**
	 * expandAveragesSeq
	 * 
	 * @param averageSeq
	 * @param countsSeq
	 * @return Map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#expand-averages-seq")
	public static <K1, K2, M1, M2> Map<K1, Map<K2, Pair<Object, Object>>> expandAveragesSeq(
			List<Map<K1, Map<K2, M1>>> averageSeq,
			List<Map<K1, Map<K2, M2>>> countsSeq) {

		Iterator<Map<K1, Map<K2, M1>>> avgItr = averageSeq.iterator();
		Iterator<Map<K1, Map<K2, M2>>> couItr = countsSeq.iterator();

		Map<K1, Map<K2, Pair<Object, Object>>> ret = new HashMap<K1, Map<K2, Pair<Object, Object>>>();

		while (avgItr.hasNext() && couItr.hasNext()) {
			Map<K1, Map<K2, Pair<Object, Object>>> tmp = expandAverages(
					avgItr.next(), couItr.next());

			for (Map.Entry<K1, Map<K2, Pair<Object, Object>>> entry : tmp
					.entrySet()) {
				K1 key = entry.getKey();
				Map<K2, Pair<Object, Object>> value = entry.getValue();
				if (ret.containsKey(key)) {
					Map<K2, Pair<Object, Object>> original = ret.get(key);
					for (Map.Entry<K2, Pair<Object, Object>> v : value
							.entrySet()) {
						K2 vk = v.getKey();
						Pair<Object, Object> vv = v.getValue();
						Pair<Object, Object> tmpPair = original.containsKey(vk) ? addPairs(
								original.get(vk), vv) : vv;
						original.put(vk, tmpPair);
					}
				} else {
					ret.put(key, value);
				}
			}
		}
		return ret;
	}

	/**
	 * valAvg
	 * 
	 * @param pair
	 * @return valavg
	 */
	@ClojureClass(className = "backtype.storm.ui.core#val-avg")
	public static Object valAvg(Pair<Object, Object> pair) {
		if (pair == null) {
			return 0;
		}
		return CoreUtil.divide(pair.getFirst(), pair.getSecond());
	}

	/**
	 * aggregateAverages
	 * 
	 * @param averageSeq
	 * @param countsSeq
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-averages")
	public static <K1, K2, M1, M2> Map<Object, Map<Object, Object>> aggregateAverages(
			List<Map<K1, Map<K2, M1>>> averageSeq,
			List<Map<K1, Map<K2, M2>>> countsSeq) {
		Map<Object, Map<Object, Object>> ret = new HashMap<Object, Map<Object, Object>>();
		Map<K1, Map<K2, Pair<Object, Object>>> expands = expandAveragesSeq(
				averageSeq, countsSeq);
		for (Map.Entry<K1, Map<K2, Pair<Object, Object>>> entry : expands
				.entrySet()) {
			Map<K2, Pair<Object, Object>> value = entry.getValue();
			Map<Object, Object> tmpAvgs = new HashMap<Object, Object>();
			for (Map.Entry<K2, Pair<Object, Object>> tmp : value.entrySet()) {
				tmpAvgs.put(tmp.getKey(), valAvg(tmp.getValue()));
			}
			ret.put(entry.getKey(), tmpAvgs);
		}
		return ret;
	}

	/**
	 * aggregateCounts
	 * 
	 * @param countsSeq
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-counts")
	@SuppressWarnings("unchecked")
	public static <K, V> Map<Object, Map<Object, Object>> aggregateCounts(
			List<Map<String, Map<K, V>>> countsSeq) {
		Map<Object, Map<Object, Object>> result = new HashMap<Object, Map<Object, Object>>();
		for (Map<String, Map<K, V>> listEntry : countsSeq) {
			if (listEntry == null) {
				continue;
			}
			for (Map.Entry<String, Map<K, V>> me : listEntry.entrySet()) {
				String key = me.getKey();
				Map<K, V> value = me.getValue();
				Map<Object, Object> valueObj = new HashMap<Object, Object>();
				for (Map.Entry<K, V> entry : value.entrySet()) {
					valueObj.put(entry.getKey(), entry.getValue());
				}
				if (result.containsKey(key)) {
					Map<Object, Object> tmp = result.get(key);
					result.put(key, CoreUtil.mergeWith(tmp, valueObj));
				} else {
					result.put(key, valueObj);
				}
			}
		}
		return result;

	}

	/**
	 * aggregateAvgStreams
	 * 
	 * @param avg
	 * @param counts
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-avg-streams")
	public static <K1, K2, M1, M2> Map<K1, Object> aggregateAvgStreams(
			Map<K1, Map<K2, M1>> avg, Map<K1, Map<K2, M2>> counts) {

		Map<K1, Object> ret = new HashMap<K1, Object>();

		Map<K1, Map<K2, Pair<Object, Object>>> expanded = expandAverages(avg,
				counts);
		for (Map.Entry<K1, Map<K2, Pair<Object, Object>>> entry : expanded
				.entrySet()) {
			Map<K2, Pair<Object, Object>> streams = entry.getValue();
			Pair<Object, Object> tmpPair = null;
			for (Pair<Object, Object> v : streams.values()) {
				tmpPair = addPairs(tmpPair, v);
			}
			ret.put(entry.getKey(), valAvg(tmpPair));
		}
		return ret;
	}

	/**
	 * aggregateCountStreams
	 * 
	 * @param stats
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-count-streams")
	public static Map<Object, Object> aggregateCountStreams(
			Map<Object, Map<Object, Object>> stats) {
		Map<Object, Object> ret = new HashMap<Object, Object>();
		if (null != stats) {
			for (Map.Entry<Object, Map<Object, Object>> entry : stats
					.entrySet()) {
				Map<Object, Object> values = entry.getValue();
				Object tmpCounts = 0L;
				for (Object value : values.values()) {
					tmpCounts = CoreUtil.add(tmpCounts, value);
				}
				ret.put(entry.getKey(), tmpCounts);
			}
		}

		return ret;
	}

	/**
	 * aggregateCommonStats
	 * 
	 * @param statsSeq
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-common-stats")
	public static Map<StatsFields, Map<Object, Map<Object, Object>>> aggregateCommonStats(
			List<ExecutorStats> statsSeq) {

		List<Map<String, Map<String, Long>>> emittedList = new ArrayList<Map<String, Map<String, Long>>>();
		List<Map<String, Map<String, Long>>> transferrdList = new ArrayList<Map<String, Map<String, Long>>>();
		for (ExecutorStats e : statsSeq) {
			emittedList.add(e.get_emitted());
			transferrdList.add(e.get_transferred());
		}
		Map<StatsFields, Map<Object, Map<Object, Object>>> ret = new HashMap<StatsFields, Map<Object, Map<Object, Object>>>();

		ret.put(StatsFields.emitted, aggregateCounts(emittedList));
		ret.put(StatsFields.transferred, aggregateCounts(transferrdList));
		return ret;

	}

	/**
	 * mkIncludeSysFn
	 * 
	 * @param includeSys
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#mk-include-sys-fn")
	public static IncludeSysFn mkIncludeSysFn(boolean includeSys) {
		return new IncludeSysFn(includeSys);
	}

	/**
	 * isAckStream
	 * 
	 * @param stream
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#is-ack-stream")
	public static boolean isAckStream(String stream) {
		if (stream.equals(Common.ACKER_INIT_STREAM_ID)
				|| stream.equals(Common.ACKER_ACK_STREAM_ID)
				|| stream.equals(Common.ACKER_FAIL_STREAM_ID)) {
			return true;
		}
		return false;
	}

	/**
	 * preProcess
	 * 
	 * @param streamSummary
	 * @param includeSys
	 */
	@ClojureClass(className = "backtype.storm.ui.core#pre-process")
	public static void preProcess(
			Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary,
			boolean includeSys) {
		IncludeSysFn filterFn = mkIncludeSysFn(includeSys);
		Map<Object, Map<Object, Object>> emitted = streamSummary
				.get(StatsFields.emitted);
		for (Map.Entry<Object, Map<Object, Object>> entry : emitted.entrySet()) {
			Object window = entry.getKey();
			Map<Object, Object> stat = entry.getValue();
			emitted.put(window, CoreUtil.filterKey(filterFn, stat));
		}
		Map<Object, Map<Object, Object>> transferred = streamSummary
				.get(StatsFields.transferred);
		for (Map.Entry<Object, Map<Object, Object>> entry : transferred
				.entrySet()) {
			Object window = entry.getKey();
			Map<Object, Object> stat = entry.getValue();
			transferred.put(window, CoreUtil.filterKey(filterFn, stat));
		}
	}

	/**
	 * aggregateBoltStats
	 * 
	 * @param statsSeq
	 * @param includeSys
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-bolt-stats")
	public static Map<StatsFields, Map<Object, Map<Object, Object>>> aggregateBoltStats(
			List<ExecutorStats> statsSeq, boolean includeSys) {
		Map<StatsFields, Map<Object, Map<Object, Object>>> result = new HashMap<StatsFields, Map<Object, Map<Object, Object>>>();
		// common
		Map<StatsFields, Map<Object, Map<Object, Object>>> common = aggregateCommonStats(statsSeq);

		List<Map<String, Map<GlobalStreamId, Long>>> ackedList = new ArrayList<Map<String, Map<GlobalStreamId, Long>>>();
		List<Map<String, Map<GlobalStreamId, Long>>> failedList = new ArrayList<Map<String, Map<GlobalStreamId, Long>>>();
		List<Map<String, Map<GlobalStreamId, Long>>> executedList = new ArrayList<Map<String, Map<GlobalStreamId, Long>>>();
		List<Map<String, Map<GlobalStreamId, Double>>> processLatenciesList = new ArrayList<Map<String, Map<GlobalStreamId, Double>>>();
		List<Map<String, Map<GlobalStreamId, Double>>> executeLatencies = new ArrayList<Map<String, Map<GlobalStreamId, Double>>>();

		for (ExecutorStats stats : statsSeq) {
			ExecutorSpecificStats specific = stats.get_specific();
			if (specific.is_set_bolt()) {
				BoltStats boltStats = specific.get_bolt();

				ackedList.add(boltStats.get_acked());
				failedList.add(boltStats.get_failed());
				executedList.add(boltStats.get_executed());
				processLatenciesList.add(boltStats.get_process_ms_avg());
				executeLatencies.add(boltStats.get_execute_ms_avg());
			}
		}

		result.putAll(common);
		result.put(StatsFields.acked, aggregateCounts(ackedList));
		result.put(StatsFields.failed, aggregateCounts(failedList));
		result.put(StatsFields.executed, aggregateCounts(executedList));
		result.put(StatsFields.process_latencies,
				aggregateAverages(processLatenciesList, ackedList));
		result.put(StatsFields.execute_latencies,
				aggregateAverages(executeLatencies, executedList));
		return result;

	}

	/**
	 * aggregateSpoutStats
	 * 
	 * @param statsSeq
	 * @param includeSys
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-spout-stats")
	public static Map<StatsFields, Map<Object, Map<Object, Object>>> aggregateSpoutStats(
			List<ExecutorStats> statsSeq, boolean includeSys) {
		Map<StatsFields, Map<Object, Map<Object, Object>>> ret = new HashMap<StatsFields, Map<Object, Map<Object, Object>>>();
		Map<StatsFields, Map<Object, Map<Object, Object>>> commons = aggregateCommonStats(statsSeq);
		preProcess(commons, includeSys);
		ret.putAll(commons);
		// acked
		List<Map<String, Map<String, Long>>> acked = new ArrayList<Map<String, Map<String, Long>>>();
		List<Map<String, Map<String, Long>>> failed = new ArrayList<Map<String, Map<String, Long>>>();
		List<Map<String, Map<String, Double>>> completeLatencies = new ArrayList<Map<String, Map<String, Double>>>();
		for (ExecutorStats es : statsSeq) {
			ExecutorSpecificStats specific = es.get_specific();
			if (specific.is_set_spout()) {
				SpoutStats spoutStats = specific.get_spout();
				acked.add(spoutStats.get_acked());
				failed.add(spoutStats.get_failed());
				completeLatencies.add(spoutStats.get_complete_ms_avg());
			}
		}

		ret.put(StatsFields.acked, aggregateCounts(acked));
		ret.put(StatsFields.failed, aggregateCounts(failed));
		ret.put(StatsFields.complete_latencies,
				aggregateAverages(completeLatencies, acked));
		return ret;
	}

	/**
	 * aggregateBoltStreams
	 * 
	 * @param stats
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-bolt-streams")
	public static Map<StatsFields, Map<Object, Object>> aggregateBoltStreams(
			Map<StatsFields, Map<Object, Map<Object, Object>>> stats) {
		Map<StatsFields, Map<Object, Object>> ret = new HashMap<StatsFields, Map<Object, Object>>();
		ret.put(StatsFields.acked,
				aggregateCountStreams(stats.get(StatsFields.acked)));
		ret.put(StatsFields.failed,
				aggregateCountStreams(stats.get(StatsFields.failed)));
		ret.put(StatsFields.emitted,
				aggregateCountStreams(stats.get(StatsFields.emitted)));
		ret.put(StatsFields.transferred,
				aggregateCountStreams(stats.get(StatsFields.transferred)));
		ret.put(StatsFields.process_latencies,
				aggregateAvgStreams(stats.get(StatsFields.process_latencies),
						stats.get(StatsFields.acked)));
		ret.put(StatsFields.executed,
				aggregateCountStreams(stats.get(StatsFields.executed)));
		ret.put(StatsFields.execute_latencies,
				aggregateAvgStreams(stats.get(StatsFields.execute_latencies),
						stats.get(StatsFields.executed)));
		return ret;
	}

	/**
	 * aggregateSpoutStreams
	 * 
	 * @param stats
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#aggregate-spout-streams")
	public static Map<StatsFields, Map<Object, Object>> aggregateSpoutStreams(
			Map<StatsFields, Map<Object, Map<Object, Object>>> stats) {
		Map<StatsFields, Map<Object, Object>> ret = new HashMap<StatsFields, Map<Object, Object>>();

		ret.put(StatsFields.acked,
				aggregateCountStreams(stats.get(StatsFields.acked)));
		ret.put(StatsFields.failed,
				aggregateCountStreams(stats.get(StatsFields.failed)));
		ret.put(StatsFields.emitted,
				aggregateCountStreams(stats.get(StatsFields.emitted)));
		ret.put(StatsFields.transferred,
				aggregateCountStreams(stats.get(StatsFields.transferred)));
		ret.put(StatsFields.complete_latencies,
				aggregateAvgStreams(stats.get(StatsFields.complete_latencies),
						stats.get(StatsFields.acked)));

		return ret;
	}

	/**
	 * check ExecutorSummary if Spout type
	 * 
	 * @param topology
	 * @param s
	 * @return true if is spout, else false
	 */
	@ClojureClass(className = "backtype.storm.ui.core#spout-summary?")
	public static boolean isSpoutSummary(StormTopology topology,
			ExecutorSummary s) {
		return executorSummaryType(topology, s).equals(ExecutorType.spout);
	}

	/**
	 * isBoltSummary
	 * 
	 * @param topology
	 * @param s
	 * @return true if is bolt, else false
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-summary?")
	public static boolean isBoltSummary(StormTopology topology,
			ExecutorSummary s) {
		return executorSummaryType(topology, s).equals(ExecutorType.bolt);
	}

	/**
	 * groupByComp
	 * 
	 * @param summs
	 * @return map
	 */
	@ClojureClass(className = "backtype.storm.ui.core#group-by-comp")
	public static Map<String, List<ExecutorSummary>> groupByComp(
			List<ExecutorSummary> summs) {
		Map<String, List<ExecutorSummary>> sortedMap = new TreeMap<String, List<ExecutorSummary>>();

		for (ExecutorSummary summ : summs) {
			String componentId = summ.get_component_id();
			if (sortedMap.containsKey(componentId)) {
				sortedMap.get(componentId).add(summ);
			} else {
				List<ExecutorSummary> sums = new ArrayList<ExecutorSummary>();
				sums.add(summ);
				sortedMap.put(componentId, sums);
			}
		}
		return sortedMap;
	}

	/**
	 * errorSubset
	 * 
	 * @param errorStr
	 * @return string
	 */
	@ClojureClass(className = "backtype.storm.ui.core#error-subset")
	public static String errorSubset(String errorStr) {
		return errorStr.substring(0, 200);
	}

	/**
	 * mostRecentError
	 * 
	 * @param errors
	 * @return the most recent ErrorInfo
	 */
	@ClojureClass(className = "backtype.storm.ui.core#most-recent-error")
	public static ErrorInfo mostRecentError(List<ErrorInfo> errors) {
		int max = 0;
		ErrorInfo result = new ErrorInfo();
		for (ErrorInfo error : errors) {
			int errorTime = error.get_error_time_secs();
			if (errorTime > max) {
				max = errorTime;
				result = error;
			}
		}
		return result;
	}

	/**
	 * componentTaskSumms
	 * 
	 * @param summ
	 * @param topology
	 * @param id
	 * @return list
	 */
	@ClojureClass(className = "backtype.storm.ui.core#component-task-summs")
	public static List<ExecutorSummary> componentTaskSumms(TopologyInfo summ,
			StormTopology topology, String id) {
		List<ExecutorSummary> executorSumms = summ.get_executors();
		List<ExecutorSummary> spoutSumms = new ArrayList<ExecutorSummary>();
		List<ExecutorSummary> boltSumms = new ArrayList<ExecutorSummary>();

		if (CollectionUtils.isNotEmpty(executorSumms)) {
			for (ExecutorSummary es : executorSumms) {
				if (isSpoutSummary(topology, es)) {
					spoutSumms.add(es);
				} else if (isBoltSummary(topology, es)) {
					boltSumms.add(es);
				}
			}
		}
		Map<String, List<ExecutorSummary>> spoutCompSumms = groupByComp(spoutSumms);
		Map<String, List<ExecutorSummary>> boltCompSumms = groupByComp(boltSumms);

		List<ExecutorSummary> ret = new ArrayList<ExecutorSummary>();
		if (spoutCompSumms.containsKey(id)) {
			ret = spoutCompSumms.get(id);
		} else {
			ret = boltCompSumms.get(id);
		}

		if (null != ret) {
			Collections.sort(ret, new Comparator<ExecutorSummary>() {

				@Override
				public int compare(ExecutorSummary o1, ExecutorSummary o2) {
					return o1.get_executor_info().get_task_start()
							- o2.get_executor_info().get_task_start();
				}

			});
		}
		return ret;
	}

	/**
	 * workerLogLink
	 * 
	 * @param host
	 * @param workerPort
	 * @return worker log link string eg:
	 *         http://localhost:8081/file=worker-6701.log
	 */
	@ClojureClass(className = "backtype.storm.ui.core#worker-log-link")
	public static String workerLogLink(String host, int workerPort) {
		String workerLogLink = Helpers.urlFormat(
				"http://%s:%s/logs/worker-%s.log", host,
				CoreUtil.parseInt(STORM_CONF.get(Config.LOGVIEWER_PORT), 8081),
				workerPort);
		return workerLogLink;
	}

	/**
	 * computeExecutorCapacity
	 * 
	 * @param e
	 * @return executor capacity
	 */
	@ClojureClass(className = "backtype.storm.ui.core#compute-executor-capacity")
	public static Double computeExecutorCapacity(ExecutorSummary e) {
		Map<StatsFields, Object> stats = new HashMap<StatsFields, Object>();
		ExecutorStats executorStats = e.get_stats();
		if (executorStats != null) {
			Map<StatsFields, Map<Object, Object>> boltStreams = aggregateBoltStreams(aggregateBoltStats(
					Lists.newArrayList(executorStats), true));
			Map<Object, Map<StatsFields, Object>> swapMapOrder = Helpers
					.swapMapOrder(boltStreams);
			stats = swapMapOrder.get("600");
		}
		int uptime = e.get_uptime_secs();
		int window = 0;
		if (uptime < 600) {
			window = uptime;
		} else {
			window = 600;
		}

		long executed = CoreUtil.parseLong(stats.get(StatsFields.executed), 0L);
		Double latency = Double.valueOf(String.valueOf(stats
				.get(StatsFields.execute_latencies)));

		Double result = 0.0;
		if (window > 0) {
			result = Double.valueOf((executed * latency) / (1000 * window));
		}
		return result;

	}

	/**
	 * computeBoltCapacity
	 * 
	 * @param executors
	 * @return bolt capacity
	 */
	@ClojureClass(className = "backtype.storm.ui.core#compute-bolt-capacity")
	public static Double computeBoltCapacity(List<ExecutorSummary> executors) {
		Double max = 0.0;
		for (ExecutorSummary e : executors) {
			Double executorCapacity = computeExecutorCapacity(e);
			if (executorCapacity != null && executorCapacity > max) {
				max = executorCapacity;
			}
		}
		return max;

	}

	/**
	 * getErrorTime
	 * 
	 * @param error
	 * @return error time
	 */
	@ClojureClass(className = "backtype.storm.ui.core#get-error-time")
	public static int getErrorTime(ErrorInfo error) {
		if (error != null) {
			return CoreUtil.timeDelta(error.get_error_time_secs());
		}
		return 0;
	}

	/**
	 * getErrorData
	 * 
	 * @param error
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#get-error-data")
	public static String getErrorData(ErrorInfo error) {
		if (error != null) {
			return errorSubset(error.get_error());
		}
		return null;
	}

	/**
	 * getErrorPort
	 * 
	 * @param error
	 * @param errorHost
	 * @param topId
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#get-error-port")
	public static int getErrorPort(ErrorInfo error, String errorHost,
			String topId) {
		if (error != null) {
			return error.get_port();
		}
		return 0;
	}

	/**
	 * getErrorHost
	 * 
	 * @param error
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#get-error-host")
	public static String getErrorHost(ErrorInfo error) {
		if (error != null) {
			return error.get_host();
		}
		return "";
	}

	/**
	 * spoutStreamsStats
	 * 
	 * @param summs
	 * @param includeSys
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#spout-streams-stats")
	public static Map<StatsFields, Map<Object, Object>> spoutStreamsStats(
			List<ExecutorSummary> summs, boolean includeSys) {
		List<ExecutorStats> statsSeq = getFilledStats(summs);
		return aggregateSpoutStreams(aggregateSpoutStats(statsSeq, includeSys));

	}

	/**
	 * boltStreamsStats
	 * 
	 * @param summs
	 * @param includeSys
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-streams-stats")
	public static Map<StatsFields, Map<Object, Object>> boltStreamsStats(
			List<ExecutorSummary> summs, boolean includeSys) {
		List<ExecutorStats> statsSeq = getFilledStats(summs);
		return aggregateBoltStreams(aggregateBoltStats(statsSeq, includeSys));

	}

	/**
	 * totalAggregateStats
	 * 
	 * @param spoutSumms
	 * @param boltSumms
	 * @param includeSys
	 * @param dumpGenerator
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#total-aggregate-stats")
	@SuppressWarnings("unchecked")
	public static Map<StatsFields, Map<Object, Object>> totalAggregateStats(
			List<ExecutorSummary> spoutSumms, List<ExecutorSummary> boltSumms,
			boolean includeSys, JsonGenerator dumpGenerator) {
		Map<StatsFields, Map<Object, Object>> aggSpoutStats = spoutStreamsStats(
				spoutSumms, includeSys);
		Map<StatsFields, Map<Object, Object>> aggBoltStats = boltStreamsStats(
				boltSumms, includeSys);

		Map<StatsFields, Map<Object, Object>> result = aggSpoutStats;
		// Include only keys that will be used. We want to count acked and
		// failed only for the "tuple trees," so we do not include those keys
		// from the bolt executors.
		result.put(StatsFields.emitted, CoreUtil.mergeWith(
				aggBoltStats.get(StatsFields.emitted),
				aggSpoutStats.get(StatsFields.emitted)));
		result.put(StatsFields.transferred, CoreUtil.mergeWith(
				aggBoltStats.get(StatsFields.transferred),
				aggSpoutStats.get(StatsFields.transferred)));

		return result;

	}

	/**
	 * 
	 * @param statsMap
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#stats-times")
	public static Set<Integer> statsTimes(Map<Object, Object> statsMap) {
		Set<Integer> windows = new TreeSet<Integer>();
		for (Object window : statsMap.keySet()) {
			windows.add(CoreUtil.parseInt(window));
		}
		return windows;
	}

	/**
	 * 
	 * @param window
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#window-hint")
	public static String windowHint(String window) {
		int wind = Integer.valueOf(window).intValue();
		return windowHint(wind);
	}

	public static String windowHint(int window) {
		if (window == 0) {
			return "All time";
		}
		return Helpers.prettyUptimeSec(window);
	}

	/**
	 * sanitizeStreamName
	 * 
	 * @param name
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#sanitize-stream-name")
	public static String sanitizeStreamName(String name) {
		if (REFIND_P.matcher(name).matches()) {
			name = name.replaceAll(SYM_REGEX, "_");
		} else {
			name = ("s" + name).replaceAll(SYM_REGEX, "_");
		}
		return String.valueOf(name.hashCode());
	}

	/**
	 * sanitizeTransferred
	 * 
	 * @param transferred
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#sanitize-transferred")
	public static void sanitizeTransferred(
			Map<String, Map<String, Long>> transferred,
			JsonGenerator dumpGenerator) throws JsonGenerationException,
			IOException {
		dumpGenerator.writeStartArray();
		for (Map.Entry<String, Map<String, Long>> entry : transferred
				.entrySet()) {
			String time = entry.getKey();
			Map<String, Long> streamMap = entry.getValue();
			dumpGenerator.writeFieldName(time);
			dumpGenerator.writeStartObject();
			dumpGenerator.writeStartArray();
			for (Map.Entry<String, Long> stream : streamMap.entrySet()) {
				dumpGenerator.writeStartObject();
				dumpGenerator.writeNumberField(
						sanitizeStreamName(stream.getKey()), stream.getValue());
				dumpGenerator.writeEndObject();
			}
			dumpGenerator.writeEndArray();
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * visualizationData
	 * 
	 * @param spouts
	 * @param bolts
	 * @param spoutCompSumms
	 * @param boltCompSumms
	 * @param window
	 * @param stormId
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#visualization-data")
	public static void visualizationData(Map<String, SpoutSpec> spouts,
			Map<String, Bolt> bolts,
			Map<String, List<ExecutorSummary>> spoutCompSumms,
			Map<String, List<ExecutorSummary>> boltCompSumms, String window,
			String stormId, boolean streamBoxes, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {

		dumpGenerator.writeStartArray();
		Set<Map<String, String>> boxes = new HashSet<Map<String, String>>();
		for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
			String id = entry.getKey();
			SpoutSpec spec = entry.getValue();
			Map<GlobalStreamId, Grouping> inputs = spec.get_common()
					.get_inputs();
			if (!streamBoxes) {
				List<ExecutorSummary> boltSumms = boltCompSumms.get(id);
				List<ExecutorSummary> spoutSumms = spoutCompSumms.get(id);
				double boltCap = null != boltSumms ? computeBoltCapacity(boltSumms)
						: 0d;
				dumpGenerator.writeStartObject();
				dumpGenerator.writeStringField("type",
						null != boltSumms ? "bolt" : "spout");
				dumpGenerator.writeStringField("capacity",
						String.valueOf(boltCap));
				String latency = null != boltSumms ? String
						.valueOf(boltStreamsStats(boltSumms, true).get(
								StatsFields.process_latencies).get(window))
						: String.valueOf(spoutStreamsStats(spoutSumms, true)
								.get(window));
				dumpGenerator.writeStringField("latency", latency);
				Object transferred = null;
				if (null != spoutSumms) {
					transferred = spoutStreamsStats(spoutSumms, true).get(
							StatsFields.transferred).get(window);
				}
				if (null == transferred) {
					transferred = boltStreamsStats(boltSumms, true).get(
							StatsFields.transferred).get(window);
				}
				dumpGenerator.writeNumberField("transferred",
						CoreUtil.parseLong(transferred, 0L));
				dumpGenerator.writeFieldName("stats");
				if (null != boltSumms) {
					mapFn(boltSumms, dumpGenerator);
				} else {
					mapFn(spoutSumms, dumpGenerator);
				}
				// inputs
				dumpGenerator.writeFieldName("inputs");
				dumpGenerator.writeStartArray();
				for (Map.Entry<GlobalStreamId, Grouping> input : inputs
						.entrySet()) {
					GlobalStreamId gId = input.getKey();
					String streamId = gId.get_streamId();
					dumpGenerator.writeStringField("component",
							gId.get_componentId());
					dumpGenerator.writeStringField("stream", streamId);
					dumpGenerator.writeStringField("sani-stream",
							sanitizeStreamName(streamId));
					dumpGenerator.writeStringField("grouping", Thrift
							.groupingType(input.getValue()).getName());
				}
				dumpGenerator.writeEndArray();
				dumpGenerator.writeEndObject();
			} else {
				for (Map.Entry<GlobalStreamId, Grouping> input : inputs
						.entrySet()) {
					Map<String, String> tmp = new HashMap<String, String>();
					GlobalStreamId gId = input.getKey();
					String streamId = gId.get_streamId();
					tmp.put("stream", streamId);
					tmp.put("sani-stream", sanitizeStreamName(streamId));
					tmp.put("checked", String.valueOf(isAckStream(streamId)));
					boxes.add(tmp);
				}
			}
		}
		if (streamBoxes) {
			writeJsonForStreamBox(boxes, dumpGenerator);
		}
		dumpGenerator.writeEndArray();
	}

	private static void writeJsonForStreamBox(Set<Map<String, String>> streams,
			JsonGenerator dumpGenerator) throws JsonGenerationException,
			IOException {
		// TODO
		// int i =
		// int num = 0;
		// for (Map<String, String> stream : streams) {
		// if (num < 4) {
		// if (num == 0) {
		// dumpGenerator.writeFieldName("row");
		// dumpGenerator.writeStartObject();
		// }
		// for (Map.Entry<String, String> entry : stream.entrySet()) {
		// dumpGenerator.writeStringField(entry.getKey(), entry.getValue());
		// }
		// if (num == 3) {
		// dumpGenerator.writeEndObject();
		// num = 0;
		// }
		// }
		// }
	}

	/**
	 * 
	 * @param dat
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#visualization-data#mapfn")
	private static void mapFn(List<ExecutorSummary> dat,
			JsonGenerator dumpGenerator) throws JsonGenerationException,
			IOException {
		dumpGenerator.writeStartArray();
		for (ExecutorSummary e : dat) {
			dumpGenerator.writeStartObject();
			dumpGenerator.writeStringField("host", e.get_host());
			dumpGenerator.writeNumberField("port", e.get_port());
			dumpGenerator.writeNumberField("uptimes_secs", e.get_uptime_secs());
			ExecutorStats stats = e.get_stats();
			if (null != stats) {
				sanitizeTransferred(stats.get_transferred(), dumpGenerator);
			}
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * mk-visualization-data
	 * 
	 * @throws TException
	 * 
	 */
	@ClojureClass(className = "backtype.storm.ui.core#mk-visualization-data")
	public static void mkVisualizationData(String id, String window,
			boolean includeSys, OutputStreamWriter out) throws TException {

		NimbusClient client = withNimbus();
		window = (window == null) ? "0" : window;
		try {
			StormTopology topology = client.getClient().getTopology(id);
			Map<String, SpoutSpec> spouts = topology.get_spouts();
			Map<String, Bolt> bolts = topology.get_bolts();
			TopologyInfo summ = client.getClient().getTopologyInfo(id);
			List<ExecutorSummary> execs = summ.get_executors();
			List<ExecutorSummary> spoutSumms = new ArrayList<ExecutorSummary>();
			List<ExecutorSummary> boltSumms = new ArrayList<ExecutorSummary>();
			for (ExecutorSummary exec : execs) {
				if (isSpoutSummary(topology, exec)) {
					spoutSumms.add(exec);
				} else {
					boltSumms.add(exec);
				}
			}
			Map<String, List<ExecutorSummary>> spoutCompSumms = groupByComp(spoutSumms);
			Map<String, List<ExecutorSummary>> boltCompSumms = groupByComp(boltSumms);
			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
			dumpGenerator.writeStartObject();
			visualizationData(spouts, bolts, spoutCompSumms, boltCompSumms,
					window, id, false, dumpGenerator);
			dumpGenerator.writeEndObject();
			dumpGenerator.flush();

		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		} finally {
			if (client != null) {
				client.close();
			}
		}

	}

	/**
	 * Generate Json Cluster Configuration
	 * 
	 * @param out
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#cluster-configuration")
	@SuppressWarnings("unchecked")
	public static void clusterConfiguration(OutputStreamWriter out)
			throws TException {
		NimbusClient client = withNimbus();

		try {
			String jsonConf = client.getClient().getNimbusConf();
			Map<String, Object> config = (Map<String, Object>) CoreUtil
					.from_json(jsonConf);
			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
			dumpGenerator.flush();
			synchronized (config) {
				dumpGenerator.writeStartObject();
				for (Map.Entry<String, Object> item : config.entrySet()) {
					dumpGenerator.writeStringField(
							String.valueOf(item.getKey()),
							String.valueOf(config.get((String) item.getKey())));
				}
				dumpGenerator.writeEndObject();
			}
			dumpGenerator.flush();
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * Generate Json Cluster Summary
	 * 
	 * @param out
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#cluster-summary")
	public static void clusterSummary(OutputStreamWriter out) throws TException {
		NimbusClient client = withNimbus();

		try {
			ClusterSummary summ = client.getClient().getClusterInfo();
			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
			dumpGenerator.flush();
			synchronized (summ) {
				dumpGenerator.writeStartObject();
				List<SupervisorSummary> sups = summ.get_supervisors();
				List<TopologySummary> tops = summ.get_topologies();
				Integer usedSlots = 0, totalSlots = 0, freeSlots = 0, totalTasks = 0, totalExecutors = 0;
				for (SupervisorSummary sup : sups) {
					usedSlots += sup.get_num_used_workers();
					totalSlots += sup.get_num_workers();
				}
				freeSlots = totalSlots - usedSlots;
				for (TopologySummary top : tops) {
					totalTasks += top.get_num_tasks();
					totalExecutors += top.get_num_executors();
				}
				// Storm version
				dumpGenerator.writeStringField("stormVersion",
						Core.readStormVersion());
				// Shows how long the cluster is running
				dumpGenerator.writeStringField("nimbusUptime",
						Helpers.prettyUptimeSec(summ.get_nimbus_uptime_secs()));
				// Number of supervisors running
				dumpGenerator.writeNumberField("supervisors", sups.size());
				// Total number of available worker slots
				dumpGenerator.writeNumberField("slotsTotal", totalSlots);
				// Number of worker slots used
				dumpGenerator.writeNumberField("slotsUsed", usedSlots);
				// Number of worker slots available
				dumpGenerator.writeNumberField("slotsFree", freeSlots);
				// Total number of executors
				dumpGenerator
						.writeNumberField("executorsTotal", totalExecutors);
				// Total tasks
				dumpGenerator.writeNumberField("tasksTotal", totalTasks);

			}
			dumpGenerator.writeEndObject();
			dumpGenerator.flush();
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * Generate Supervisor Summary
	 * 
	 * @param out
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#supervisor-summary")
	public static void supervisorSummary(OutputStreamWriter out)
			throws TException {
		NimbusClient client = withNimbus();

		try {
			List<SupervisorSummary> summs = client.getClient().getClusterInfo()
					.get_supervisors();
			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
			dumpGenerator.writeStartObject();
			dumpGenerator.writeFieldName("supervisors");
			dumpGenerator.writeStartArray();
			dumpGenerator.flush();
			synchronized (summs) {
				for (SupervisorSummary s : summs) {
					dumpGenerator.writeStartObject();
					// Supervisor's id
					dumpGenerator.writeStringField("id", s.get_supervisor_id());
					// Supervisor's host name
					dumpGenerator.writeStringField("host", s.get_host());
					// Shows how long the supervisor is running
					dumpGenerator.writeStringField("uptime",
							Helpers.prettyUptimeSec(s.get_uptime_secs()));
					// Total number of available worker slots for this
					// supervisor
					dumpGenerator.writeNumberField("slotsTotal",
							s.get_num_workers());
					// Number of worker slots used on this supervisor
					dumpGenerator.writeNumberField("slotsUsed",
							s.get_num_used_workers());
					dumpGenerator.writeEndObject();
				}
			}
			dumpGenerator.writeEndArray();
			dumpGenerator.writeEndObject();
			dumpGenerator.flush();
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static void supervisorConf(String supervisorId, Writer out)
			throws TException {
		// NimbusClient client = withNimbus();
		//
		// try {
		// String supervisorConf =
		// client.getClient().getSupervisorConf(supervisorId);
		// Map<String, Object> config =
		// (Map<String, Object>) CoreUtil.from_json(supervisorConf);
		// JsonFactory dumpFactory = new JsonFactory();
		// JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
		// dumpGenerator.writeStartObject();
		// dumpGenerator.writeFieldName("configuration");
		// synchronized (config) {
		// dumpGenerator.writeStartObject();
		// for (Map.Entry<String, Object> item : config.entrySet()) {
		// dumpGenerator.writeStringField(String.valueOf(item.getKey()),
		// String.valueOf(config.get((String) item.getKey())));
		// }
		// dumpGenerator.writeEndObject();
		// }
		// dumpGenerator.writeEndObject();
		// dumpGenerator.flush();
		// } catch (Exception e) {
		// throw new TException(CoreUtil.stringifyError(e));
		// } finally {
		// if (client != null) {
		// client.close();
		// }
		// }
	}

	public static void supervisorWorkers(String host, OutputStreamWriter out)
			throws TException {
		//
		// NimbusClient client = withNimbus();
		// try {
		// SupervisorWorkers workers =
		// client.getClient().getSupervisorWorkers(host);
		// SupervisorSummary s = null;
		// List<SupervisorSummary> summs =
		// client.getClient().getClusterInfo().get_supervisors();
		// for (SupervisorSummary summ : summs) {
		// if (summ.get_host().equals(host)) {
		// s = summ;
		// }
		// }
		// JsonFactory dumpFactory = new JsonFactory();
		// JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
		// dumpGenerator.writeStartObject();
		// if (s != null) {
		// dumpGenerator.writeStringField("id", s.get_supervisor_id());
		// dumpGenerator.writeStringField("host", s.get_host());
		// dumpGenerator.writeNumberField("uptime", s.get_uptime_secs());
		// dumpGenerator.writeNumberField("slotsTotal", s.get_num_workers());
		// dumpGenerator.writeNumberField("slotsUsed",
		// s.get_num_used_workers());
		// }
		// dumpGenerator.writeFieldName("workers");
		// dumpGenerator.writeStartArray();
		// dumpGenerator.flush();
		// synchronized (workers) {
		// for (WorkerSummary e : workers.get_workers()) {
		//
		// StringBuilder taskSB = new StringBuilder();
		// StringBuilder componentSB = new StringBuilder();
		// boolean isFirst = true;
		// int minUptime = 0;
		// for (ExecutorSummary executorSummary : e.get_tasks()) {
		// if (isFirst == false) {
		// taskSB.append(',');
		// componentSB.append(',');
		// } else {
		// minUptime = executorSummary.get_uptime_secs();
		// }
		// taskSB.append(executorSummary.get_executor_info().get_task_start());
		// componentSB.append(executorSummary.get_component_id());
		//
		// if (minUptime < executorSummary.get_uptime_secs()) {
		// minUptime = executorSummary.get_uptime_secs();
		// }
		//
		// isFirst = false;
		// }
		//
		// dumpGenerator.writeStartObject();
		// dumpGenerator.writeNumberField("port", e.get_port());
		// dumpGenerator.writeStringField("uptime",
		// Helpers.prettyUptimeSec(minUptime));
		// dumpGenerator.writeStringField("topology", e.get_topology());
		// dumpGenerator.writeStringField("taskList", taskSB.toString());
		// dumpGenerator.writeStringField("componentList",
		// componentSB.toString());
		// dumpGenerator.writeEndObject();
		// }
		// }
		// dumpGenerator.writeEndArray();
		// dumpGenerator.writeEndObject();
		// dumpGenerator.flush();
		// } catch (Exception e) {
		// throw new TException(CoreUtil.stringifyError(e));
		// } finally {
		// if (client != null) {
		// client.close();
		// }
		// }
	}

	/**
	 * allTopologiesSummary
	 * 
	 * @param out
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#all-topologies-summary")
	public static void allTopologiesSummary(OutputStreamWriter out)
			throws TException {
		NimbusClient client = withNimbus();

		try {
			List<TopologySummary> summs = client.getClient().getClusterInfo()
					.get_topologies();

			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
			dumpGenerator.writeStartObject();
			dumpGenerator.writeFieldName("topologies");
			dumpGenerator.writeStartArray();
			dumpGenerator.flush();
			synchronized (summs) {
				for (TopologySummary t : summs) {
					dumpGenerator.writeStartObject();
					// Topology Id
					dumpGenerator.writeStringField("id", t.get_id());
					// Encoded Topology Id
					dumpGenerator.writeStringField("encodedId",
							CoreUtil.urlEncode(t.get_id()));
					// Topology Name
					dumpGenerator.writeStringField("name", t.get_name());
					// Topology Status
					dumpGenerator.writeStringField("status", t.get_status());
					// Shows how long the topology is running
					dumpGenerator.writeStringField("uptime",
							Helpers.prettyUptimeSec(t.get_uptime_secs()));
					// Total number of tasks for this topology
					dumpGenerator.writeNumberField("tasksTotal",
							t.get_num_tasks());
					// Number of workers used for this topology
					dumpGenerator.writeNumberField("workersTotal",
							t.get_num_workers());
					// Number of executors used for this topology
					dumpGenerator.writeNumberField("executorsTotal",
							t.get_num_executors());
					dumpGenerator.writeEndObject();
				}
			}
			dumpGenerator.writeEndArray();
			dumpGenerator.writeEndObject();
			dumpGenerator.flush();
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * Array of all the topology related stats per time window
	 * 
	 * @param id
	 * @param window
	 * @param stats
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#topology-stats")
	private static void topologyStats(String id, String window,
			Map<StatsFields, Map<Object, Object>> stats,
			JsonGenerator dumpGenerator) throws IOException,
			JsonGenerationException {
		Set<Integer> times = statsTimes(stats.get(StatsFields.emitted));
		dumpGenerator.writeFieldName("topologyStats");
		dumpGenerator.writeStartArray();
		for (Integer k : times) {
			dumpGenerator.writeStartObject();
			// Duration passed in HH:MM:SS format
			dumpGenerator.writeStringField("windowPretty",
					windowHint(k.intValue()));
			// User requested time window for metrics
			dumpGenerator.writeNumberField("window", k);
			// Number of messages emitted in given window
			dumpGenerator.writeNumberField(
					"emitted",
					CoreUtil.parseLong(
							stats.get(StatsFields.emitted).get(
									String.valueOf(k)), 0L));
			// Number messages transferred in given window
			dumpGenerator.writeNumberField(
					"transferred",
					CoreUtil.parseLong(
							stats.get(StatsFields.transferred).get(
									String.valueOf(k)), 0L));
			// Total latency for processing the message
			dumpGenerator.writeStringField(
					"completeLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.complete_latencies).get(
									String.valueOf(k)), 0.00f)));
			// Number of messages acked in given window
			dumpGenerator.writeNumberField("acked", CoreUtil.parseLong(stats
					.get(StatsFields.acked).get(String.valueOf(k)), 0L));
			// Number of messages failed in given window
			dumpGenerator.writeNumberField("failed", CoreUtil.parseLong(stats
					.get(StatsFields.failed).get(String.valueOf(k)), 0L));
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();

	}

	/**
	 * Array of all the spout components in the topology
	 * 
	 * @param topId
	 * @param summMap
	 * @param errors
	 * @param window
	 * @param includeSys
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#spout-comp")
	private static void spoutComp(String topId,
			Map<String, List<ExecutorSummary>> summMap,
			Map<String, List<ErrorInfo>> errors, String window,
			boolean includeSys, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {
		dumpGenerator.writeFieldName("spouts");
		dumpGenerator.writeStartArray();
		for (Map.Entry<String, List<ExecutorSummary>> e : summMap.entrySet()) {
			String id = e.getKey();
			List<ExecutorSummary> summs = e.getValue();
			List<ExecutorStats> statsSeq = getFilledStats(summs);
			Map<StatsFields, Map<Object, Object>> stats = aggregateSpoutStreams(aggregateSpoutStats(
					statsSeq, includeSys));
			ErrorInfo lastError = mostRecentError(errors.get(id));
			String errorHost = getErrorHost(lastError);
			int errorPort = getErrorPort(lastError, errorHost, topId);
			dumpGenerator.writeStartObject();
			// Spout id
			dumpGenerator.writeStringField("spoutId", id);
			// Encoded Spout id
			dumpGenerator.writeStringField("encodedSpoutId",
					CoreUtil.urlEncode(id));
			// Number of executors for the spout
			dumpGenerator.writeNumberField("executors", summs.size());
			// Total number of tasks for the spout
			dumpGenerator.writeNumberField("tasks", Helpers.sumTasks(summs));
			// Number of messages emitted in given window
			dumpGenerator.writeNumberField(
					"emitted",
					CoreUtil.parseLong(
							stats.get(StatsFields.emitted).get(window), 0L));
			// Total number of messages transferred in given window
			dumpGenerator
					.writeNumberField(
							"transferred",
							CoreUtil.parseLong(
									stats.get(StatsFields.transferred).get(
											window), 0L));
			// Total latency for processing the message

			dumpGenerator.writeStringField(
					"completeLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.complete_latencies).get(
									window), 0.00f)));
			// Number of messages acked
			dumpGenerator.writeNumberField("acked", CoreUtil.parseLong(stats
					.get(StatsFields.acked).get(window), 0L));
			// Number of messages failed
			dumpGenerator.writeNumberField("failed", CoreUtil.parseLong(stats
					.get(StatsFields.failed).get(window), 0L));
			// Error worker Hostname
			dumpGenerator.writeStringField("errorHost", errorHost);
			// Error worker port
			dumpGenerator.writeNumberField("errorPort", errorPort);
			// Link to the worker log that reported the exception
			dumpGenerator.writeStringField("errorWorkerLogLink",
					workerLogLink(errorHost, errorPort));
			// Number of seconds elapsed since that last error happened in a
			// spout
			dumpGenerator.writeNumberField("errorLapsedSecs",
					getErrorTime(lastError));
			// Shows the last error happened in a spout
			dumpGenerator.writeStringField("lastError", lastError.get_error());
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * Array of bolt components in the topology
	 * 
	 * @param topId
	 * @param summMap
	 * @param errors
	 * @param window
	 * @param includeSys
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-comp")
	private static void boltComp(String topId,
			Map<String, List<ExecutorSummary>> summMap,
			Map<String, List<ErrorInfo>> errors, String window,
			boolean includeSys, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {
		dumpGenerator.writeFieldName("bolts");
		dumpGenerator.writeStartArray();
		for (Map.Entry<String, List<ExecutorSummary>> e : summMap.entrySet()) {
			String id = e.getKey();
			List<ExecutorSummary> summs = e.getValue();
			List<ExecutorStats> statsSeq = getFilledStats(summs);
			Map<StatsFields, Map<Object, Object>> stats = aggregateBoltStreams(aggregateBoltStats(
					statsSeq, includeSys));
			ErrorInfo lastError = mostRecentError(errors.get(id));
			String errorHost = getErrorHost(lastError);
			int errorPort = getErrorPort(lastError, errorHost, topId);
			dumpGenerator.writeStartObject();
			// Bolt id
			dumpGenerator.writeStringField("boltId", id);
			// Encoded Bolt id
			dumpGenerator.writeStringField("encodedBoltId",
					CoreUtil.urlEncode(id));
			// Number of executor tasks in the bolt component
			dumpGenerator.writeNumberField("executors", summs.size());
			// Number of instances of bolt
			dumpGenerator.writeNumberField("tasks", Helpers.sumTasks(summs));
			// Number of tuples emitted
			Map<Object, Object> res = stats.get(StatsFields.emitted);
			dumpGenerator.writeNumberField("emitted", res == null ? 0L
					: CoreUtil.parseLong(res.get(window), 0L));
			// Total number of messages transferred in given window
			res = stats.get(StatsFields.transferred);
			dumpGenerator.writeNumberField("transferred", res == null ? 0
					: CoreUtil.parseLong(res.get(window), 0L));
			// This value indicates number of messages executed * average
			// execute
			// latency / time window

			dumpGenerator
					.writeStringField("capacity", Helpers.floatStr(CoreUtil
							.parseFloat(
									String.valueOf(computeBoltCapacity(summs)),
									0.00f)));
			// Average time for bolt's execute method
			// TODO
			res = stats.get(StatsFields.execute_latencies);
			dumpGenerator.writeStringField(
					"executeLatency",
					res == null ? "0.00" : Helpers.floatStr(CoreUtil
							.parseFloat(res.get(window), 0.00f)));
			// Total number of messages executed in given window
			res = stats.get(StatsFields.executed);
			dumpGenerator.writeNumberField("executed", res == null ? 0
					: CoreUtil.parseLong(res.get(window), 0L));
			// Bolt's average time to ack a message after it's received
			res = stats.get(StatsFields.process_latencies);
			dumpGenerator.writeStringField(
					"processLatency",
					res == null ? "0.00" : Helpers.floatStr(CoreUtil
							.parseFloat(res.get(window), 0.00f)));
			// Number of tuples acked by the bolt
			res = stats.get(StatsFields.acked);
			dumpGenerator.writeNumberField("acked", (res == null) ? 0
					: CoreUtil.parseLong(res.get(window), 0L));
			// Number of tuples failed by the bolt
			res = stats.get(StatsFields.failed);
			dumpGenerator.writeNumberField("failed", (res == null) ? 0
					: CoreUtil.parseLong(res.get(window), 0L));
			// Error worker Hostname
			dumpGenerator.writeStringField("errorHost", errorHost);
			// Error worker port
			dumpGenerator.writeNumberField("errorPort", errorPort);
			// Link to the worker log that reported the exception
			dumpGenerator.writeStringField("errorWorkerLogLink",
					workerLogLink(errorHost, errorPort));
			// Number of seconds elapsed since that last error happened in a
			// bolt
			dumpGenerator.writeNumberField("errorLapsedSecs",
					getErrorTime(lastError));
			// Shows the last error occurred in the bolt
			dumpGenerator.writeStringField("lastError", lastError.get_error());
			dumpGenerator.writeEndObject();
		}

		dumpGenerator.writeEndArray();
	}

	/**
	 * topologySummary
	 * 
	 * @param summ
	 * @param dumpGenerator
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#topology-summary")
	private static void topologySummary(TopologyInfo summ,
			JsonGenerator dumpGenerator) throws TException {
		List<ExecutorSummary> executors = summ.get_executors();
		Set<WorkerSlot> workers = new HashSet<WorkerSlot>();
		for (ExecutorSummary e : executors) {
			workers.add(new WorkerSlot(e.get_host(), e.get_port()));
		}

		try {
			// Topology Id
			dumpGenerator.writeStringField("id", summ.get_id());
			// Encoded Topology Id
			dumpGenerator.writeStringField("encodedId",
					CoreUtil.urlEncode(summ.get_id()));
			// Topology Name
			dumpGenerator.writeStringField("name", summ.get_name());
			// Shows Topology's current status
			dumpGenerator.writeStringField("status", summ.get_status());
			// Shows how long the topology is running
			dumpGenerator.writeStringField("uptime",
					Helpers.prettyUptimeSec(summ.get_uptime_secs()));
			// Total number of tasks for this topology
			dumpGenerator.writeNumberField("tasksTotal",
					Helpers.sumTasks(executors));
			// Number of workers used for this topology
			dumpGenerator.writeNumberField("workersTotal", workers.size());
			// Number of executors used for this topology
			dumpGenerator.writeNumberField("executorsTotal", executors.size());
		} catch (IOException e) {
			throw new TException(CoreUtil.stringifyError(e));
		}
	}

	/**
	 * spoutSummaryJson
	 * 
	 * @param topologyId
	 * @param id
	 * @param stats
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#spout-summary-json")
	public static void spoutSummaryJson(String topologyId, String id,
			Map<StatsFields, Map<Object, Object>> stats,
			JsonGenerator dumpGenerator) throws JsonGenerationException,
			IOException {
		Set<Integer> times = statsTimes(stats.get(StatsFields.emitted));
		dumpGenerator.writeFieldName("spoutSummary");
		dumpGenerator.writeStartArray();
		for (Integer k : times) {
			String windowStr = String.valueOf(k);

			dumpGenerator.writeStartObject();
			dumpGenerator.writeStringField("windowPretty",
					windowHint(k.intValue()));
			dumpGenerator.writeNumberField("window", k);
			dumpGenerator.writeNumberField(
					"emitted",
					CoreUtil.parseLong(
							stats.get(StatsFields.emitted).get(windowStr), 0L));
			dumpGenerator.writeNumberField("transferred", CoreUtil.parseLong(
					stats.get(StatsFields.transferred).get(windowStr), 0L));
			dumpGenerator.writeStringField(
					"completeLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.complete_latencies).get(
									windowStr), 0.00f)));
			dumpGenerator.writeNumberField(
					"acked",
					CoreUtil.parseLong(
							stats.get(StatsFields.acked).get(windowStr), 0L));
			dumpGenerator.writeNumberField(
					"failed",
					CoreUtil.parseLong(
							stats.get(StatsFields.failed).get(windowStr), 0L));
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();

	}

	/**
	 * topologyPage
	 * 
	 * @param id
	 * @param window
	 * @param includeSys
	 * @param out
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#topology-page")
	@SuppressWarnings("unchecked")
	public static void topologyPage(String id, String window,
			boolean includeSys, OutputStreamWriter out) throws TException {
		NimbusClient client = withNimbus();
		try {
			TopologyInfo summ = client.getClient().getTopologyInfo(id);
			StormTopology topology = client.getClient().getTopology(id);
			String topologyConf = client.getClient().getTopologyConf(id);
			Map<String, Object> stormConf = (Map<String, Object>) CoreUtil
					.from_json(topologyConf);
			List<ExecutorSummary> executorSummarys = summ.get_executors();
			List<ExecutorSummary> spoutSumms = new ArrayList<ExecutorSummary>();
			List<ExecutorSummary> boltSumms = new ArrayList<ExecutorSummary>();

			for (ExecutorSummary s : executorSummarys) {
				if (Core.isSpoutSummary(topology, s)) {
					spoutSumms.add(s);
				} else if (Core.isBoltSummary(topology, s)) {
					boltSumms.add(s);
				}
			}
			Map<String, List<ExecutorSummary>> spoutCompSumms = Core
					.groupByComp(spoutSumms);
			Map<String, List<ExecutorSummary>> boltCompSumms = Core
					.groupByComp(boltSumms);
			Map<String, SpoutSpec> spouts = topology.get_spouts();
			Map<String, Bolt> bolts = topology.get_bolts();

			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
			dumpGenerator.writeStartObject();
			topologySummary(summ, dumpGenerator);
			// window param value defalut value is "0"
			dumpGenerator.writeStringField("window", window);
			// window param value in "hh mm ss" format. Default value is
			// "All Time"
			dumpGenerator.writeStringField("windowHint",
					Core.windowHint(window));
			// Number of seconds a tuple has before the spout considers it
			// failed
			dumpGenerator.writeNumberField("msgTimeout", CoreUtil.parseInt(
					stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30));

			// topology-stats
			// Array of all the topology related stats per time window
			Map<StatsFields, Map<Object, Object>> stats = totalAggregateStats(
					spoutSumms, boltSumms, includeSys, dumpGenerator);
			topologyStats(id, window, stats, dumpGenerator);
			// spouts
			spoutComp(id, spoutCompSumms, summ.get_errors(), window,
					includeSys, dumpGenerator);
			// bolts
			boltComp(id, boltCompSumms, summ.get_errors(), window, includeSys,
					dumpGenerator);
			// configuration
			topologyConf(stormConf, dumpGenerator);

			// visualizationTable
			dumpGenerator.writeFieldName("visualizationTable");
			visualizationData(spouts, bolts, boltCompSumms, boltCompSumms,
					window, id, true, dumpGenerator);

			dumpGenerator.writeEndObject();
			dumpGenerator.flush();
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		} finally {
			if (client != null) {
				client.close();
			}
		}

	}

	/**
	 * spoutOutputStats
	 * 
	 * @param streamSummary
	 * @param window
	 * @return
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#topology-page#spout-output-stats")
	public static void spoutOutputStats(
			Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary,
			String window, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {

		Map<Object, Map<StatsFields, Map<Object, Object>>> swap = Helpers
				.swapMapOrder(streamSummary);
		Map<Object, Map<Object, Map<StatsFields, Object>>> tmp = new HashMap<Object, Map<Object, Map<StatsFields, Object>>>();
		for (Map.Entry<Object, Map<StatsFields, Map<Object, Object>>> entry : swap
				.entrySet()) {
			tmp.put(entry.getKey(), Helpers.swapMapOrder(entry.getValue()));
		}
		dumpGenerator.writeFieldName("outputStats");
		dumpGenerator.writeStartArray();
		Map<Object, Map<StatsFields, Object>> windowsStat = tmp.get(window);
		for (Map.Entry<Object, Map<StatsFields, Object>> entry : windowsStat
				.entrySet()) {
			Map<StatsFields, Object> stat = entry.getValue();
			dumpGenerator.writeStartObject();
			dumpGenerator.writeObjectField("stream", entry.getKey());
			dumpGenerator.writeNumberField("emitted",
					CoreUtil.parseLong(stat.get(StatsFields.emitted), 0L));
			dumpGenerator.writeNumberField("transferred",
					CoreUtil.parseLong(stat.get(StatsFields.transferred), 0L));
			dumpGenerator.writeStringField(
					"completeLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stat.get(StatsFields.complete_latencies), 0.00f)));
			dumpGenerator.writeNumberField("acked",
					CoreUtil.parseLong(stat.get(StatsFields.acked), 0L));
			dumpGenerator.writeNumberField("failed",
					CoreUtil.parseLong(stat.get(StatsFields.failed), 0L));
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * spoutExecutorStats
	 * 
	 * @param topologyId
	 * @param executors
	 * @param window
	 * @param includeSys
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#spout-executor-stats")
	public static void spoutExecutorStats(String topologyId,
			List<ExecutorSummary> executors, String window, boolean includeSys,
			JsonGenerator dumpGenerator) throws JsonGenerationException,
			IOException {

		dumpGenerator.writeFieldName("executorStats");
		dumpGenerator.writeStartArray();
		for (ExecutorSummary e : executors) {
			Map<StatsFields, Object> stats = new HashMap<StatsFields, Object>();
			ExecutorStats executorStats = e.get_stats();
			if (stats != null) {
				Map<StatsFields, Map<Object, Object>> spoutStreamsMap = aggregateSpoutStreams(aggregateSpoutStats(
						Lists.newArrayList(executorStats), includeSys));
				Map<Object, Map<StatsFields, Object>> swapMapOrder = Helpers
						.swapMapOrder(spoutStreamsMap);
				stats = swapMapOrder.get(window);
			}

			dumpGenerator.writeStartObject();
			dumpGenerator.writeStringField("id",
					Helpers.prettyExecutorInfo(e.get_executor_info()));
			dumpGenerator.writeStringField("encodedId",
					CoreUtil.urlEncode(Helpers.prettyExecutorInfo(e
							.get_executor_info())));
			dumpGenerator.writeStringField("uptime",
					Helpers.prettyUptimeSec(e.get_uptime_secs()));
			dumpGenerator.writeStringField("host", e.get_host());
			dumpGenerator.writeNumberField("port", e.get_port());
			dumpGenerator.writeNumberField("emitted",
					CoreUtil.parseLong(stats.get(StatsFields.emitted), 0L));
			dumpGenerator.writeNumberField("transferred",
					CoreUtil.parseLong(stats.get(StatsFields.transferred), 0L));
			dumpGenerator.writeStringField(
					"completeLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.complete_latencies), 0.00f)));
			dumpGenerator.writeNumberField("acked",
					CoreUtil.parseLong(stats.get(StatsFields.acked), 0L));
			dumpGenerator.writeNumberField("failed",
					CoreUtil.parseLong(stats.get(StatsFields.failed), 0L));
			dumpGenerator.writeStringField("workerLogLink",
					workerLogLink(e.get_host(), e.get_port()));
			dumpGenerator.writeEndObject();

		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * List of component errors
	 * 
	 * @param errorsList
	 * @param topologyId
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#component-errors")
	public static void componentErrors(List<ErrorInfo> errorsList,
			String topologyId, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {
		dumpGenerator.writeFieldName("componentErrors");
		dumpGenerator.writeStartArray();

		for (ErrorInfo e : errorsList) {
			dumpGenerator.writeStartObject();
			// Timestamp when the exception occurred
			dumpGenerator.writeNumberField("time",
					1000 * e.get_error_time_secs());
			// host name for the error
			dumpGenerator.writeStringField("errorHost", e.get_host());
			// port for the error
			dumpGenerator.writeNumberField("errorPort", e.get_port());
			// Link to the worker log that reported the exception
			dumpGenerator.writeStringField("errorWorkerLogLink",
					workerLogLink(e.get_host(), e.get_port()));
			dumpGenerator.writeNumberField("errorLapsedSecs", getErrorTime(e));
			// Shows the error happened in a component
			dumpGenerator.writeStringField("error", e.get_error());
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();

	}

	/**
	 * spoutStats
	 * 
	 * @param windows
	 * @param topologyInfo
	 * @param component
	 * @param executors
	 * @param includeSys
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#spout-stats")
	public static void spoutStats(String windows, TopologyInfo topologyInfo,
			String component, List<ExecutorSummary> executors,
			boolean includeSys, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {
		// String windowHint = "(" + windowHint(windows) + ")";
		List<ExecutorStats> stats = getFilledStats(executors);
		Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary = aggregateSpoutStats(
				stats, includeSys);
		Map<StatsFields, Map<Object, Object>> summary = aggregateSpoutStreams(streamSummary);

		spoutSummaryJson(topologyInfo.get_id(), component, summary,
				dumpGenerator);
		spoutOutputStats(streamSummary, windows, dumpGenerator);
		spoutExecutorStats(topologyInfo.get_id(), executors, windows,
				includeSys, dumpGenerator);
	}

	/**
	 * boltSummary
	 * 
	 * @param topologyId
	 * @param id
	 * @param stats
	 * @param window
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-summary")
	public static void boltSummary(String topologyId, String id,
			Map<StatsFields, Map<Object, Object>> stats, String window,
			JsonGenerator dumpGenerator) throws JsonGenerationException,
			IOException {
		Set<Integer> times = statsTimes(stats.get(StatsFields.emitted));
		dumpGenerator.writeFieldName("boltStats");
		dumpGenerator.writeStartArray();
		for (Integer k : times) {
			String windowStr = String.valueOf(k);
			dumpGenerator.writeStartObject();
			dumpGenerator.writeNumberField("window", k);
			dumpGenerator.writeStringField("windowPretty",
					Helpers.prettyUptimeSec(k));
			dumpGenerator.writeNumberField(
					"emitted",
					CoreUtil.parseLong(
							stats.get(StatsFields.emitted).get(windowStr), 0L));
			dumpGenerator.writeNumberField("transferred", CoreUtil.parseLong(
					stats.get(StatsFields.transferred).get(windowStr), 0L));
			dumpGenerator.writeStringField(
					"executeLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.execute_latencies).get(
									windowStr), 0.00f)));
			dumpGenerator
					.writeNumberField("executed", CoreUtil.parseLong(
							stats.get(StatsFields.executed).get(windowStr), 0L));
			dumpGenerator.writeStringField(
					"processLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.process_latencies).get(
									windowStr), 0.00f)));
			dumpGenerator.writeNumberField(
					"acked",
					CoreUtil.parseLong(
							stats.get(StatsFields.acked).get(windowStr), 0L));
			dumpGenerator.writeNumberField(
					"failed",
					CoreUtil.parseLong(
							stats.get(StatsFields.failed).get(windowStr), 0L));
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * boltSummary
	 * 
	 * @param streamSummay
	 * @param window
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-output-stats")
	public static void boltOutputStats(
			Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummay,
			String window, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {
		Map<StatsFields, Map<Object, Object>> statsFields = Helpers
				.swapMapOrder(streamSummay).get(window);
		Map<Object, Map<StatsFields, Object>> streams = Helpers
				.swapMapOrder(CoreUtil.select_keys(
						statsFields,
						new HashSet<StatsFields>(Arrays.asList(
								StatsFields.emitted, StatsFields.transferred))));

		dumpGenerator.writeFieldName("outputStats");
		dumpGenerator.writeStartArray();
		for (Map.Entry<Object, Map<StatsFields, Object>> entry : streams
				.entrySet()) {
			Object s = entry.getKey();
			Map<StatsFields, Object> stats = entry.getValue();

			dumpGenerator.writeStartObject();
			dumpGenerator.writeObjectField("stream", String.valueOf(s));
			dumpGenerator.writeNumberField("emitted",
					CoreUtil.parseLong(stats.get(StatsFields.emitted), 0L));
			dumpGenerator.writeNumberField("transferred",
					CoreUtil.parseLong(stats.get(StatsFields.transferred), 0L));
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * boltOutputStats
	 * 
	 * @param streamSummay
	 * @param window
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-input-stats")
	public static void boltInputStats(
			Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummay,
			String window, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {
		Map<StatsFields, Map<Object, Object>> statsFields = Helpers
				.swapMapOrder(streamSummay).get(window);
		Set<StatsFields> selectKeys = new HashSet<StatsFields>(Arrays.asList(
				StatsFields.acked, StatsFields.failed,
				StatsFields.process_latencies, StatsFields.executed,
				StatsFields.execute_latencies));
		Map<Object, Map<StatsFields, Object>> streams = Helpers
				.swapMapOrder(CoreUtil.select_keys(statsFields, selectKeys));

		dumpGenerator.writeFieldName("inputStats");
		dumpGenerator.writeStartArray();
		for (Map.Entry<Object, Map<StatsFields, Object>> entry : streams
				.entrySet()) {
			GlobalStreamId s = (GlobalStreamId) entry.getKey();
			Map<StatsFields, Object> stats = entry.getValue();

			dumpGenerator.writeStartObject();
			dumpGenerator.writeStringField("component", s.get_componentId());
			dumpGenerator.writeStringField("encodedComponent",
					CoreUtil.urlDecode(s.get_componentId()));
			dumpGenerator.writeStringField("stream", s.get_streamId());

			dumpGenerator.writeStringField(
					"executeLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.execute_latencies), 0.00f)));
			dumpGenerator.writeStringField(
					"processLatency",
					Helpers.floatStr(CoreUtil.parseFloat(
							stats.get(StatsFields.process_latencies), 0.00f)));
			dumpGenerator.writeNumberField("executed",
					CoreUtil.parseLong(stats.get(StatsFields.executed), 0L));
			dumpGenerator.writeNumberField("acked",
					CoreUtil.parseLong(stats.get(StatsFields.acked), 0L));
			dumpGenerator.writeNumberField("failed",
					CoreUtil.parseLong(stats.get(StatsFields.failed), 0L));
			dumpGenerator.writeEndObject();
		}
		dumpGenerator.writeEndArray();
	}

	/**
	 * boltExecutorStats
	 * 
	 * @param topologyId
	 * @param executors
	 * @param window
	 * @param includeSys
	 * @param dumpGenerator
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-executor-stats")
	public static void boltExecutorStats(String topologyId,
			List<ExecutorSummary> executors, String window, boolean includeSys,
			JsonGenerator dumpGenerator) throws JsonGenerationException,
			IOException {
		dumpGenerator.writeFieldName("executorStats");
		dumpGenerator.writeStartArray();
		for (ExecutorSummary e : executors) {
			ExecutorStats stats = e.get_stats();
			Map<StatsFields, Object> statsMap = new HashMap<StatsFields, Object>();
			if (stats != null) {
				Map<StatsFields, Map<Object, Object>> statsFeilds = aggregateBoltStreams(aggregateBoltStats(
						Arrays.asList(stats), includeSys));
				statsMap = Helpers.swapMapOrder(statsFeilds).get(window);
			}

			dumpGenerator.writeStartObject();
			ExecutorInfo executorInfo = e.get_executor_info();
			String id = Helpers.prettyExecutorInfo(executorInfo);
			dumpGenerator.writeStringField("id", id);
			dumpGenerator.writeStringField("encodeId", CoreUtil.urlEncode(id));
			dumpGenerator.writeStringField("uptime",
					Helpers.prettyUptimeSec(e.get_uptime_secs()));
			dumpGenerator.writeStringField("host", e.get_host());
			dumpGenerator.writeNumberField("port", e.get_port());
			dumpGenerator.writeNumberField("emitted", CoreUtil.parseLong(
					CoreUtil.mapValue(statsMap, StatsFields.emitted), 0L));
			dumpGenerator.writeNumberField("transferred", CoreUtil.parseLong(
					CoreUtil.mapValue(statsMap, StatsFields.transferred), 0L));
			dumpGenerator
					.writeStringField("capacity", Helpers.floatStr(CoreUtil
							.parseFloat(
									String.valueOf(computeExecutorCapacity(e)),
									0.00f)));
			dumpGenerator.writeStringField("executeLatency", Helpers
					.floatStr(CoreUtil.parseFloat(CoreUtil.mapValue(statsMap,
							StatsFields.execute_latencies), 0.00f)));
			dumpGenerator.writeNumberField("executed", CoreUtil.parseLong(
					CoreUtil.mapValue(statsMap, StatsFields.executed), 0L));
			dumpGenerator.writeStringField("processLatency", Helpers
					.floatStr(CoreUtil.parseFloat(CoreUtil.mapValue(statsMap,
							StatsFields.process_latencies), 0.00f)));
			dumpGenerator
					.writeNumberField("acked", CoreUtil.parseLong(
							CoreUtil.mapValue(statsMap, StatsFields.acked), 0L));
			dumpGenerator.writeNumberField("failed", CoreUtil.parseLong(
					CoreUtil.mapValue(statsMap, StatsFields.failed), 0L));
			dumpGenerator.writeStringField("workerLogLink",
					workerLogLink(e.get_host(), e.get_port()));

			dumpGenerator.writeEndObject();
		}

		dumpGenerator.writeEndArray();
	}

	/**
	 * bolt-stats
	 * 
	 * @param window
	 * @param summ
	 * @param component
	 * @param summs
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#bolt-stats")
	public static void boltStats(String window, TopologyInfo topologyInfo,
			String component, List<ExecutorSummary> executors,
			boolean includeSys, JsonGenerator dumpGenerator)
			throws JsonGenerationException, IOException {

		List<ExecutorStats> stats = getFilledStats(executors);
		Map<StatsFields, Map<Object, Map<Object, Object>>> streamSummary = aggregateBoltStats(
				stats, includeSys);
		Map<StatsFields, Map<Object, Object>> summary = aggregateBoltStreams(streamSummary);

		String topologyId = topologyInfo.get_id();

		boltSummary(topologyId, component, summary, window, dumpGenerator);
		boltInputStats(streamSummary, window, dumpGenerator);
		boltOutputStats(streamSummary, window, dumpGenerator);
		boltExecutorStats(topologyId, executors, window, includeSys,
				dumpGenerator);
	}

	/**
	 * Component Page
	 * 
	 * @param topologyId
	 * @param component
	 * @param window
	 * @param out
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#component-page")
	public static void componentPage(String topologyId, String component,
			String window, String includeSys, OutputStreamWriter out)
			throws TException {
		NimbusClient client = withNimbus();

		try {
			TopologyInfo summ = client.getClient().getTopologyInfo(topologyId);
			StormTopology topology = client.getClient().getTopology(topologyId);
			ExecutorType type = Core.componentType(topology, component);

			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);

			List<ExecutorSummary> summs = componentTaskSumms(summ, topology,
					component);

			dumpGenerator.writeStartObject();
			// Component's
			dumpGenerator.writeStringField("id", component);
			// Encode Component's
			dumpGenerator.writeStringField("encodedId",
					CoreUtil.urlEncode(component));
			// Topology name
			dumpGenerator.writeStringField("name", summ.get_name());
			// Number of executor tasks in the component
			dumpGenerator.writeNumberField("executors", summs.size());
			// Number of instances of component
			dumpGenerator.writeNumberField("tasks", Helpers.sumTasks(summs));
			// topology's id
			dumpGenerator.writeStringField("topologyId", topologyId);
			// encoded topology's id
			dumpGenerator.writeStringField("encodedTopologyId",
					CoreUtil.urlEncode(topologyId));
			dumpGenerator.writeStringField("window", window);
			// component's type SPOUT or BOLT
			dumpGenerator.writeStringField("componentType", type.getName());
			// window param value in "hh mm ss" format. Default value is
			// "All Time"
			dumpGenerator.writeStringField("windowHint", windowHint(window));
			if (type == ExecutorType.spout) {
				spoutStats(window, summ, component, summs,
						checkIncludeSys(includeSys), dumpGenerator);
			} else if (type == ExecutorType.bolt) {
				boltStats(window, summ, component, summs,
						checkIncludeSys(includeSys), dumpGenerator);
			}
			componentErrors(summ.get_errors().get(component), topologyId,
					dumpGenerator);
			dumpGenerator.writeEndObject();

			dumpGenerator.flush();
			dumpGenerator.close();
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * dumpGenerator
	 * 
	 * @param stormConf
	 * @param dumpGenerator
	 * @throws IOException
	 * @throws JsonGenerationException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#topology-page#topology-conf")
	public static void topologyConf(Map<String, Object> stormConf,
			JsonGenerator dumpGenerator) throws IOException,
			JsonGenerationException {
		dumpGenerator.writeFieldName("configuration");
		synchronized (stormConf) {
			dumpGenerator.writeStartObject();
			for (Map.Entry<String, Object> item : stormConf.entrySet()) {
				dumpGenerator.writeStringField(String.valueOf(item.getKey()),
						String.valueOf(stormConf.get((String) item.getKey())));
			}
			dumpGenerator.writeEndObject();
		}
	}

	/**
	 * checkIncludeSys
	 * 
	 * @param sys
	 * @return
	 */
	@ClojureClass(className = "backtype.storm.ui.core#check-include-sys?")
	public static boolean checkIncludeSys(String sys) {
		if (sys != null && sys.equals("true")) {
			return true;
		}
		return false;

	}

	/**
	 * exceptionToJson
	 * 
	 * @param ex
	 * @param out
	 * @throws TException
	 */
	@ClojureClass(className = "backtype.storm.ui.core#exception->json")
	public static void exceptionToJson(Exception ex, OutputStreamWriter out)
			throws TException {
		try {
			JsonFactory dumpFactory = new JsonFactory();
			JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
			dumpGenerator.writeStartObject();
			dumpGenerator.writeStringField("error", "Internal Server Error");
			dumpGenerator.writeStringField("errorMessage",
					CoreUtil.stringifyError(ex));
			dumpGenerator.writeEndObject();
			dumpGenerator.flush();
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		}
	}

	@ClojureClass(className = "backtype.storm.ui.core#main-routes#kill")
	public static void killTopology(String topologyId, int waitTime)
			throws TException {
		// TODO user check

		NimbusClient client = withNimbus();
		try {
			TopologyInfo tplg = client.getClient().getTopologyInfo(topologyId);
			String name = tplg.get_name();
			KillOptions options = new KillOptions();
			options.set_wait_secs(waitTime);

			Log.info("Killing topology {} with wait time:{}", name, waitTime);
			client.getClient().killTopologyWithOpts(name, options);
		} catch (Exception e) {
			throw new TException(CoreUtil.stringifyError(e));
		}
	}
}
