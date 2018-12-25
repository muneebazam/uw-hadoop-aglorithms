// Spark implementation of Stripes PMI Calculator
package ca.uwaterloo.cs451.a2

// imports
import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

// handle command line arguments
class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  val MAX_WORD_COUNT = 40

  def main(argv: Array[String]) {

    // print argument info
    val args = new StripesPMIConf(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of Reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    // set configuration variables
    val conf = new SparkConf().setAppName("Stripes PMI")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.output())
    val threshold = args.threshold()
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // store input data in memory
    val inputFile = sc.textFile(args.input())

    //  counts the number of word occurrences
    val counts = inputFile.flatMap(line => {
        var tokens = tokenize(line)
        tokens.take(Math.min(tokens.length, MAX_WORD_COUNT)).distinct
      })
      .map(pair => (pair, 1.0))
      .reduceByKey(_ + _)
      .collectAsMap()

    // broadcast word occurrence counts and calculate number of lines
    val countsMap = sc.broadcast(counts)
    val numLines = inputFile.count()

    // calculates all word co-occurrences creating a stripe with the following form
    // (word, {(co-occur1, co-occur1-count), (co-occur2, co-occur2-count), ...})
    inputFile.flatMap(line => {
        val tokens = tokenize(line)
        val distinctTokens = tokens.take(Math.min(tokens.length, MAX_WORD_COUNT)).distinct
        if (distinctTokens.length > 1) {
          val combinations = distinctTokens.combinations(2).flatMap(word => word.permutations)
          combinations.map(word => (word(0), Map(word(1) -> 1.0)))
        }
        else {
          List()
        }
      })
      .reduceByKey((key, value) => {
        key ++ value.map({case(k, v) => k -> (v + key.getOrElse(k, 0.0))})
      })
      .map(pair => {
        var pmiMap = Map[String, (Float, Int)]()
        var key = pair._1
        var values = pair._2
        for ((word, count) <- values) {
          if (count >= threshold) {
            // calculate PMI
            var numerator = numLines.toFloat * count
            var denominator = countsMap.value(key) *  countsMap.value(word)
            // .toFloat & .toInt types must be forced to avoid weird compile time error
            pmiMap += (word -> (Math.log10(numerator / denominator).toFloat, count.toInt))
          }
        }
        if (pmiMap.size > 0){
          (key, pmiMap)
        }
      })
      .saveAsTextFile(args.output())
  }
}
