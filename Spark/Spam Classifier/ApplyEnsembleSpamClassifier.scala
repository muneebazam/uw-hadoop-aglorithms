package ca.uwaterloo.cs451.a6

import org.apache.spark.sql.functions.broadcast
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.math.exp


class ApplyEnsembleSpamConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model, method)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "model", required = true)
    val method = opt[String](descr = "ensemble method", required = true)
    verify()
}

object ApplyEnsembleSpamClassifier {
    val log = Logger.getLogger(getClass.getName)

    def getModel(model: String): scala.collection.Map[Int,Double] = {
        val sc = SparkSession.builder.appName("ApplyEnsembleSpam").getOrCreate()

        sc.sparkContext.textFile(model)
            .map(line => {
                    val pattern = """^\((.*),(.*)\)$""".r
                    val pattern(feature, weight) = line
                    (feature.toInt, weight.toDouble)
            })
            .collectAsMap()
    }

    def main(argv: Array[String]) {
        val args = new ApplyEnsembleSpamConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Ensemble: " + args.model())
        log.info("Model: " + args.model())

        val sc = SparkSession.builder
                        .appName("ApplyEnsembleSpam")
                        .getOrCreate()

        val x_model = sc.sparkContext.broadcast(getModel(args.model() + "/part-00000"))
        val y_model = sc.sparkContext.broadcast(getModel(args.model() + "/part-00001"))
        val britney_model = sc.sparkContext.broadcast(getModel(args.model() + "/part-00002"))

        FileSystem.get(sc.sparkContext.hadoopConfiguration).delete(new Path(args.output()), true)  
        val method = args.method()

        val classified = sc.sparkContext.textFile(args.input()).map(instance => {
            def spamminess(features: Array[Int], w: scala.collection.Map[Int,Double]): Double = {
                var score: Double = 0.0
                features.foreach(f => if (w.contains(f)) score+= w(f))
                return score
            }

            var docid :: isSpam :: features = instance.split("\\s+").toList
            val feature_list = features.map(_.toInt).toArray
			val x_score = spamminess(feature_list, x_model.value)
            val y_score = spamminess(feature_list, y_model.value)
            val britney_score = spamminess(feature_list, britney_model.value)

            val score = method match {
                case "vote" => {
                    val count = List(x_score, y_score, britney_score).count(_ > 0)
                    (count - (3 - count))
                }
                case _ => (x_score + y_score + britney_score) / 3.0
            }
			(docid, isSpam, score, if (score > 0) "spam" else "ham")
        })
        classified.saveAsTextFile(args.output())
    }

}
