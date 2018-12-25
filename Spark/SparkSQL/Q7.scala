package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "Input Path", required = true)
  val date = opt[String](descr = "Date", required = true)
  val text = opt[Boolean](descr = "Text", required = false)
  val parquet = opt[Boolean](descr = "Parquet", required = false)
  verify()
}

object Q7 {
    val log = Logger.getLogger(getClass().getName())

    val c_name = 1
    val c_custkey = 0
    val o_custkey = 1
    val l_partkey = 1
    val l_orderkey = 0
    val o_orderkey = 0
    val o_orderdate = 4
    val o_ship_priority = 5
    val l_ext_price = 5
    val l_discount = 6
    val l_ship_date = 10
    val num_results = 10

  def main(argv: Array[String]) {
    val args = new Q7Conf(argv)
    val date_filter = args.date()
    val sc = SparkSession.builder
                    .appName("Q7")
                    .getOrCreate()
        
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    
    val answer = if (args.parquet()) {
        val customer = sc.read.parquet(args.input() + "/customer")
                            .rdd
                            .map(pair => (pair.getInt(c_custkey), pair.getString(c_name)))
        val customer_tmp = sc.sparkContext.broadcast(customer.collectAsMap())
        sc.read.parquet(args.input() + "/lineitem").rdd
                            .filter(_.getString(l_ship_date) > date_filter)
                            .map(pair =>
                                (pair.getInt(l_orderkey), pair.getDouble(l_ext_price) * (1.0 - pair.getDouble(l_discount)))
                            )
                            .reduceByKey(_ + _)
                            .cogroup(sc.read.parquet(args.input() + "/orders")
                                        .rdd
                                        .filter(_.getString(o_orderdate) < date_filter)
                                        .map(tup => (tup.getInt(o_orderkey),(tup.getString(o_orderdate),
                                                                tup.getString(o_ship_priority),
                                                                customer_tmp.value(tup.getInt(o_custkey))))))                                                                    
                            .filter{ case(a,b) => b._1.size != 0 && b._2.size != 0 }
                            .map{case(o_key, iters) => (iters._1.head, (iters._2.head._3,o_key, iters._2.head._1,iters._2.head._2))}
                            .sortByKey(false)
                            .take(num_results)
                            .foreach{ case(rev, (c_name, o_key, orderdate, priority)) =>
                                println(c_name, o_key, rev, orderdate, priority)
                            }
    } else if (args.text()) {
        val customer = sc.sparkContext.textFile(args.input() + "/customer.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(c_custkey).toInt, pair(c_name))) 
        val customer_tmp = sc.sparkContext.broadcast(customer.collectAsMap())
        sc.sparkContext.textFile(args.input() + "/lineitem.tbl")
                            .map(_.split('|'))
                            .filter(_(l_ship_date) > date_filter)
                            .map(pair =>
                                (pair(l_orderkey).toInt, pair(l_ext_price).toDouble * (1.0 - pair(l_discount).toDouble))
                            )
                            .reduceByKey(_ + _)
                            .cogroup(sc.sparkContext.textFile(args.input() + "/orders.tbl")
                                                .map(_.split('|'))
                                                .filter(_(o_orderdate) < date_filter)
                                                .map(tup => (tup(o_orderkey).toInt,(tup(o_orderdate),
                                                                tup(o_ship_priority),
                                                                customer_tmp.value(tup(o_custkey).toInt)))))                                                                    
                            .filter{ case(a,b) => b._1.size != 0 && b._2.size != 0 }
                            .map{case(o_key, iters) => (iters._1.head, (iters._2.head._3,o_key, iters._2.head._1,iters._2.head._2))}
                            .sortByKey(false)
                            .take(num_results)
                            .foreach{ case(rev, (c_name, o_key, orderdate, priority)) =>
                                println(c_name, o_key, rev, orderdate, priority)
                            }
    }
  }
}