package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "Input Path", required = true)
  val date = opt[String](descr = "Date", required = true)
  val text = opt[Boolean](descr = "Text", required = false)
  val parquet = opt[Boolean](descr = "Parquet", required = false)
  verify()
}

object Q3 {
    val log = Logger.getLogger(getClass().getName())
    
    val p_name = 1
    val s_name = 1
    val p_partkey = 0
    val s_suppkey = 0
    val l_partkey = 1
    val l_suppkey = 2
    val l_orderkey = 0
    val l_shipdate = 10

  def main(argv: Array[String]) {
    val args = new Q3Conf(argv)
    val date_filter = args.date()
    val num_results = 20
    val sc = SparkSession.builder
                    .appName("Q3")
                    .getOrCreate()
        
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val answer = if (args.parquet()) {
        val filtered_items = sc.read.parquet(args.input() + "/lineitem")
                                     .rdd
                                     .filter(_.getString(10).contains(date_filter))
        val part = sc.read.parquet(args.input() + "/part")
                            .rdd
                            .map(pair => (pair.getInt(p_partkey), pair.getString(p_name)))
        val part_tmp = sc.sparkContext.broadcast(part.collectAsMap())

        val supplier = sc.read.parquet(args.input() + "/supplier")
                            .rdd
                            .map(pair => (pair.getInt(s_suppkey), pair.getString(s_name)))

        val supplier_tmp = sc.sparkContext.broadcast(supplier.collectAsMap())

        filtered_items.map(pair => (pair.getInt(l_orderkey),
                                    (part_tmp.value(pair.getInt(l_partkey)), supplier_tmp.value(pair.getInt(l_suppkey)))
                                    ))
                         .sortByKey()
                         .take(num_results)
                         .foreach{ case(order_key, names) => println(order_key, names._1, names._2)}
    } else if (args.text()) {
        val filtered_items = sc.sparkContext.textFile(args.input() + "/lineitem.tbl")
                                             .map(_.split('|'))
                                             .filter(_(l_shipdate).contains(date_filter))
        val part = sc.sparkContext.textFile(args.input() + "/part.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(p_partkey).toInt, pair(p_name)))
        val part_tmp = sc.sparkContext.broadcast(part.collectAsMap())

        val supplier = sc.sparkContext.textFile(args.input() + "/supplier.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(s_suppkey).toInt, pair(s_name)))

        val supplier_tmp = sc.sparkContext.broadcast(supplier.collectAsMap())

        filtered_items.map(pair =>
                                (pair(l_orderkey).toInt,
                                            (part_tmp.value(pair(l_partkey).toInt), supplier_tmp.value(pair(l_suppkey).toInt)))
                                )
                            .sortByKey()
                            .take(num_results)
                            .foreach{ case(order_key, names) => println(order_key, names._1, names._2)}

    }
  }
}