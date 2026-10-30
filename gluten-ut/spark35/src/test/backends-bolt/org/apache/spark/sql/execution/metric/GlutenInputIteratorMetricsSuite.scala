/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.metric

import org.apache.gluten.backendsapi.bolt.BoltMetricsApi
import org.apache.gluten.metrics.{IMetrics, Metrics, MetricsUpdaterTree, MetricsUtil}
import org.apache.gluten.substrait.{AggregationParams, JoinParams}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.metrics.TaskStatsAccumulator
import org.apache.spark.sql.execution.SparkPlan

import org.mockito.Mockito.{mock, when}

import java.lang.{Long => JLong}
import java.util.{Arrays, Collections, List => JList}

class GlutenInputIteratorMetricsSuite extends SparkFunSuite {
  private def metricsUpdater(
      forBroadcast: Boolean = false,
      forShuffle: Boolean = false): (Map[String, SQLMetric], IMetrics => Unit) = {
    val sparkContext = mock(classOf[SparkContext])
    when(sparkContext.cleaner).thenReturn(None)
    val child = mock(classOf[SparkPlan])
    val outputRows = SQLMetrics.createMetric(sparkContext, "rows")
    outputRows += 42
    when(child.metrics).thenReturn(Map("numOutputRows" -> outputRows))

    val api = new BoltMetricsApi()
    val metrics = api.genInputIteratorTransformerMetrics(
      child,
      sparkContext,
      forBroadcast,
      forShuffle)
    val updater = api.genInputIteratorTransformerMetricsUpdater(metrics, forBroadcast)
    val update = MetricsUtil.genMetricsUpdatingFunction(
      MetricsUpdaterTree(updater, Seq.empty),
      Collections.singletonMap[JLong, JList[JLong]](0L, Arrays.asList[JLong](0L, 1L)),
      0L,
      Collections.emptyMap[JLong, JoinParams](),
      Collections.emptyMap[JLong, AggregationParams](),
      new TaskStatsAccumulator()
    )
    (metrics, update)
  }

  private def nativeMetrics(firstCpu: Option[Long], secondCpu: Option[Long]): Metrics = {
    def operator(cpu: Option[Long], wall: Long): String = {
      val cpuField = cpu.map(nanos => s""", "cpuNanos": $nanos""").getOrElse("")
      s"""{"cpuCount": 1, "wallNanos": $wall, "outputRows": 7,
         | "outputVectors": 1$cpuField}""".stripMargin
    }
    val json = s"""{
                  |  "orderedNodeIds": ["0"],
                  |  "omittedNodeIds": [],
                  |  "nodeStats": {
                  |    "0": {"operatorStats": [
                  |      ${operator(firstCpu, 20000000000L)},
                  |      ${operator(secondCpu, 30000000000L)}
                  |    ]}
                  |  }
                  |}""".stripMargin
    new Metrics(json, 2, null)
  }

  for (
    (name, forBroadcast, forShuffle) <- Seq(
      ("ordinary input", false, false),
      ("shuffle input", false, true),
      ("broadcast input", true, false))
  ) {
    test(s"CPU time is parsed, merged and accumulated for $name") {
      val (metrics, update) = metricsUpdater(forBroadcast, forShuffle)
      val native = nativeMetrics(Some(5000000000L), Some(7000000000L))

      update(native)
      update(native)

      assert(metrics("cpuNanos").value == 24000000000L)
      assert(metrics("wallNanos").value == 100000000000L)
      assert(metrics("cpuCount").value == 4)
      assert(metrics("numOutputRows").value == (if (forBroadcast) 42 else 14))
    }
  }

  test("missing CPU time defaults to zero") {
    val (metrics, update) = metricsUpdater()

    update(nativeMetrics(None, None))

    assert(metrics("cpuNanos").value == 0)
    assert(metrics("wallNanos").value == 50000000000L)
  }

  test("zero CPU time does not fall back to elapsed time") {
    val (metrics, update) = metricsUpdater()

    update(nativeMetrics(Some(0L), Some(0L)))

    assert(metrics("cpuNanos").value == 0)
    assert(metrics("wallNanos").value == 50000000000L)
  }

  test("missing CPU time in one operator does not discard the other operator's CPU time") {
    val (metrics, update) = metricsUpdater()

    update(nativeMetrics(None, Some(7000000000L)))

    assert(metrics("cpuNanos").value == 7000000000L)
    assert(metrics("wallNanos").value == 50000000000L)
  }

  test("cached broadcast input can report zero CPU time without changing output rows") {
    val (metrics, update) = metricsUpdater(forBroadcast = true)

    update(nativeMetrics(Some(0L), Some(0L)))

    assert(metrics("cpuNanos").value == 0)
    assert(metrics("numOutputRows").value == 42)
  }

  test("empty native metrics do not reset CPU time") {
    val (metrics, update) = metricsUpdater()
    update(nativeMetrics(Some(5000000000L), Some(7000000000L)))

    update(new Metrics(null, 0, null))

    assert(metrics("cpuNanos").value == 12000000000L)
  }
}
