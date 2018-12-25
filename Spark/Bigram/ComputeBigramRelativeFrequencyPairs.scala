// Spark implementation of ComputeBigramRelativeFrequencyPairs
package ca.uwaterloo.cs451.a2

// imports
import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

// sort map based on x in tuple (x, y)
class SortByLeftTupleWord(partitions: Int) extends Partitioner {
	override def numPartitions: Int = partitions
	override def getPartition(key: Any): Int = {
		val tuple = key.asInstanceOf[String]
		val word = tuple.split("\\s+")(0)
		(word.hashCode() & Integer.MAX_VALUE) % numPartitions
	}
}

// handle command line arguments
class PairsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    // print argument info
    val args = new PairsConf(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of Reducers: " + args.reducers())

    // set configuration variables
    val conf = new SparkConf().setAppName("Bigram Frequency Pairs")
    val sc = new SparkContext(conf)
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // store input data in memory
    val textFile = sc.textFile(args.input())
  
    // generate every pair of two words that appear together along with their count
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val count = tokens.init.map(word => List(word, "*").mkString(" ")).toList
          val tuple = tokens.sliding(2).map(word => word.mkString(" ")).toList 	
          // concatenate both list of pairs and list of counts together	
			    tuple ++ count
        } else {
          List()
        }
      })
      .map(bigram => {
        (bigram, 1)
      })
      .reduceByKey(_ + _)
      .sortByKey()
      .repartitionAndSortWithinPartitions(new SortByLeftTupleWord(args.reducers()))
      .mapPartitions(part => {
	      var marginal = 0.0
		    part.map(tuple => {
			    tuple._1.split("\\s+")(1) match {
			    case "*" => {
				    marginal = tuple._2.toFloat
            tuple
			    }
			    case _ => (tuple._1, (tuple._2.toFloat / marginal))
			    }
		    })
	    })
	    .map(part => {
        val splitTuple = part._1.split("\\s+")
        ((splitTuple(0), splitTuple(1)), part._2)
      })
      .saveAsTextFile(args.output())
  }
}
