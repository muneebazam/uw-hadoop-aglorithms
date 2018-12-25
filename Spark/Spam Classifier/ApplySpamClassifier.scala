package ca.uwaterloo.cs451.a6

import org.apache.spark.sql.functions.broadcast
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.math.exp

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model", required = true)
    val output = opt[String](descr = "output path", required = true)
    verify()
}

object ApplySpamClassifier {
    val log = Logger.getLogger(getClass.getName)

    def main(argv: Array[String]) {
        val args = new ApplySpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Model: " + args.model())

        val sc = SparkSession.builder
                        .appName("ApplySpamClassifier")
                        .getOrCreate()

        val weight =  sc.sparkContext.textFile(args.model() + "/part-00000")
                                .map(line => {
    								val pattern = """^\((.*),(.*)\)$""".r
                                    val pattern(f, w) = line
                                    (f.toInt, w.toDouble)
                                })
                                .collectAsMap()

        val textFile =  sc.sparkContext.textFile(args.input())
        FileSystem.get(sc.sparkContext.hadoopConfiguration).delete(new Path(args.output()), true) 
        
        val temp_weight = sc.sparkContext.broadcast(weight)
        val classified = textFile.map(instance => {
            var docid :: isSpam :: features = instance.split("\\s+").toList
            var score = 0.0d
			features.map(_.toInt).foreach(f => if (temp_weight.value.contains(f)) score += temp_weight.value(f))   
			(docid, isSpam, score, if (score > 0) "spam" else "ham")
        })
        classified.saveAsTextFile(args.output())
    }

}
