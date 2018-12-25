// Spark implementation of Pairs PMI Calculator
package ca.uwaterloo.cs451.a2

// imports
import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

// handle command line arguments
class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  val MAX_WORD_COUNT = 40

  def main(argv: Array[String]) {

    // print argument info
    val args = new PairsPMIConf(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of Reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    // set configuration variables
    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.output())
    val threshold = args.threshold()
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // store input data in memory
    val inputFile = sc.textFile(args.input())

    // counts the number of word occurrences 
    val counts = inputFile.flatMap(line => {
        var tokens = tokenize(line)
        tokens.take(Math.min(tokens.length, MAX_WORD_COUNT)).distinct
       })
       .map(word => (word, 1.0))
       .reduceByKey(_ + _)
       .collectAsMap()

    // broadcast word occurrence counts and calculate number of lines
    val countsMap = sc.broadcast(counts)
    val numLines = inputFile.count()

    // calculates all word co-occurrences creating a list of pairs of the form
    // ((pair1, count1), (pair2, count2), (pair3, count3) ...)
    inputFile.flatMap(line => {
        val tokens = tokenize(line)
        val distinctTokens = tokens.take(Math.min(tokens.length, MAX_WORD_COUNT)).distinct
        if (distinctTokens.length > 1) {
            distinctTokens.combinations(2).flatMap(word => word.permutations)
        } else {
            List()
        }
       })
       .map(pair => (pair, 1))
       .reduceByKey(_ + _)
       .filter(pair => pair._2 >= threshold)
       .map(entry => {
        // calculate PMI
         var pair = entry._1
         var count = entry._2
         var numerator = numLines.toFloat * count
         var denominator = countsMap.value(pair(0)) * countsMap.value(pair(1))
         (pair, (Math.log10(numerator / denominator), count.toInt))
       })
       // format output to work with check script
       .map(entry => {
         var pair = entry._1
         var count = entry._2
         ((pair(0), pair(1)), count)
       })
       .saveAsTextFile(args.output())
  }
}
