package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.math.exp

class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, model, shuffle)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model", required = true)
    val shuffle = opt[Boolean](descr = "shuffle", required = false)
    verify()
}

object TrainSpamClassifier {
    val log = Logger.getLogger(getClass.getName)
    val delta = 0.002                                           

    def main(argv: Array[String]) {
        val args = new TrainSpamClassifierConf(argv)
        log.info("Input: " + args.input())
        log.info("Model: " + args.model())
        log.info("Shuffle: " + args.shuffle())

        val sc = SparkSession.builder
                        .appName("TrainSpamClassifier")
                        .getOrCreate()

        var textFile =  sc.sparkContext.textFile(args.input())
        FileSystem.get(sc.sparkContext.hadoopConfiguration).delete(new Path(args.model()), true)

        if (args.shuffle()) {
            val range = new scala.util.Random()
            textFile = textFile
                    .map((range.nextInt, _))
                    .sortByKey()
                    .map(_._2)
        }

        val trained = textFile.map(instance => {
            var _ :: isSpam :: features = instance.split("\\s+").toList
            (0, (if (isSpam == "spam") 1 else 0, features.map(_.toInt).toArray) )
        })
        .groupByKey()
        .flatMap{ case (_, vals) =>
            val w = scala.collection.mutable.Map[Int, Double] () 

            def spamminess(features: Array[Int]): Double = {
			    var score: Double = 0.0
			    features.foreach(f => if (w.contains(f)) score+= w(f))
			    return score
		    }

            def train(isSpam: Int, features: Array[Int]){
			    var score = spamminess(features)
			    val prob: Double = 1.0 / (1.0 + exp(-score))
			    features.foreach(f => {
				    if (w.contains(f)) {
					    w(f) += (isSpam - prob) * delta
				    } else {
					    w(f) = (isSpam - prob) * delta
				    }
			    })
		    }

            vals.foreach(v => train(v._1, v._2))
            w
        }

        trained.saveAsTextFile(args.model())
    }

}
