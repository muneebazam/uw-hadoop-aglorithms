package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "Input Path", required = true)
  val date = opt[String](descr = "Date", required = true)
  val text = opt[Boolean](descr = "Text", required = false)
  val parquet = opt[Boolean](descr = "Parquet", required = false)
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)
    val date_filter = args.date()
    val sc = SparkSession.builder
                    .appName("Q1")
                    .getOrCreate()
        
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    var answer = if (args.parquet()) {
            sc.read.parquet(args.input() + "/lineitem")
                    .rdd
                    .map(_.getString(10))
                    .filter(_.contains(date_filter))
                    .count
    } else if (args.text()) {
            sc.sparkContext.textFile(args.input() + "/lineitem.tbl")
                     .map(_.split('|')(10))
                     .filter(_.contains(date_filter))
                     .count
    }
    println("ANSWER=" + answer)
  }
}