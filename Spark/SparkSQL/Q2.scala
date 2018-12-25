package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "Input Path", required = true)
  val date = opt[String](descr = "Date", required = true)
  val text = opt[Boolean](descr = "Text", required = false)
  val parquet = opt[Boolean](descr = "Parquet", required = false)
  verify()
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q2Conf(argv)
    val date_filter = args.date()
    val numResults = 20
    val sc = SparkSession.builder
                    .appName("Q2")
                    .getOrCreate()
        
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val answer = if (args.parquet()) {
            sc.read.parquet(args.input() + "/lineitem")
                    .rdd
                    .filter(_.getString(10).contains(date_filter))
                    .map(pair => (pair.getInt(0), 1))
                    .cogroup(sc.read.parquet(args.input() + "/orders")
                                    .rdd
                                    .map(pair => (pair.getInt(0), pair.getString(6))))
                    .filter(_._2._1.size > 0)
                    .sortByKey()
                    .take(numResults)
                    .map{case(clerk, k) 
                            => (k._2.head, clerk.toLong)
                        }
                    .foreach(println)
    } else if (args.text()) {
            sc.sparkContext.textFile(args.input() + "/lineitem.tbl")
                            .map(_.split('|'))
                            .filter(_(10).contains(date_filter))
                            .map(pair => (pair(0).toInt, 1))
                            .cogroup(sc.sparkContext.textFile(args.input() + "/orders.tbl")
                                                .map(_.split('|'))
                                                .map(pair => (pair(0).toInt, pair(6))))
                            .filter(_._2._1.size > 0)
                            .sortByKey()
                            .take(numResults)
                            .map{ case(clerk, k) 
                                    => (k._2.head, clerk.toLong)
                                }
                            .foreach(println)

    } 
  }
}