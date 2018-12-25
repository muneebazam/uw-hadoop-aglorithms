/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val goldmanLongMax = -74.013777
    val goldmanLongMin = -74.0144185
    val goldmanLatMax = 40.7152275
    val goldmanLatMin = 40.7138745
    val citigroupLongMax = -74.009867
    val citigroupLongMin = -74.012083
    val citigroupLatMax = 40.7217236
    val citigroupLatMin = 40.720053

    def inCitigroup(long: Double, lat: Double): Boolean = {
      long >= citigroupLongMin && long <= citigroupLongMax &&
      lat >= citigroupLatMin && lat <= citigroupLatMax
    }

    def inGoldman(long: Double, lat: Double): Boolean = {
      long >= goldmanLongMin && long <= goldmanLongMax &&
      lat >= goldmanLatMin && lat <= goldmanLatMax
    }

    def mapFunc(newTime: Time, loc: String, count: Option[Int], state: State[Tuple3[Int, String, Int]]) : Option[Tuple2[String,Tuple3[Int, String, Int]]] = {
    val newBatch = count.getOrElse(0)
    var oldBatch = if (state.exists()) (state.get())._1 else 0
    val newTimeMs = "%08d".format(newTime.milliseconds)
    var newState: Tuple3[Int, String, Int]  = (newBatch, newTimeMs, oldBatch)
    state.update(newState)

    if (newBatch >= 10 && newBatch >= (oldBatch * 2)) {
     if (loc == "goldman") println(s"Number of arrivals to Goldman Sachs has doubled from $oldBatch to $newBatch at $newTimeMs!") 
                else println(s"Number of arrivals to Citigroup has doubled from $oldBatch to $newBatch at $newTimeMs!")
    }
    Some((loc, newState))
  }
    val wc = stream.map(_.split(","))
      .map(tuple => {
        tuple(0) match {
          case "green" => (tuple(8).toDouble, tuple(9).toDouble)
          case _ => (tuple(10).toDouble, tuple(11).toDouble)
        }
      })
      .filter{case(long,lat) => (inCitigroup(long, lat) || inGoldman(long, lat))}
      .map{ case(long, lat) => if (inCitigroup(long, lat)) ("citigroup", 1) else ("goldman", 1)}
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(StateSpec.function(mapFunc _))

	  val output = args.output()
    wc.stateSnapshots()
      .foreachRDD(
        (rdd, ts) => {
          rdd.saveAsTextFile(s"$output/part-${ts.milliseconds}")
          }
      )

    wc.saveAsTextFiles(output)

    wc.foreachRDD((rdd, time) => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}