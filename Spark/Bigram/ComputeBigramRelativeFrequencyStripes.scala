// Spark implementation of ComputeBigramRelativeFrequencyStripes
package ca.uwaterloo.cs451.a2

// imports
import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

// handle command line arguments
class StripesConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    // print argument info
    val args = new StripesConf(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of Reducers: " + args.reducers())

    // set configuration variables
    val conf = new SparkConf().setAppName("Bigram Frequency Stripes")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // store input data in memory
    val textFile = sc.textFile(args.input())
  
    // Create a stripe of a word with all the words that appear with it and their counts
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          tokens.sliding(2).map(word => {
                                  (word(0), Map(word(1) -> 1.0))
                                })		
        } else {
          List()
        }
      })
      .partitionBy(new HashPartitioner(args.reducers()))
      .reduceByKey((key, value) => {
        key ++ value.map({case(k, v) => k -> (v + key.getOrElse(k, 0.0))})
      })
	    .map(word => {
        val sum = word._2.foldLeft(0.0)(_+_._2)
        (word._1, word._2.map({case (k, v) => (k, (v / sum))}).toMap)
      })
      .saveAsTextFile(args.output())
  }
}
