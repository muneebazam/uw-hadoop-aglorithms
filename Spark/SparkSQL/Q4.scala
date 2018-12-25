package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "Input Path", required = true)
  val date = opt[String](descr = "Date", required = true)
  val text = opt[Boolean](descr = "Text", required = false)
  val parquet = opt[Boolean](descr = "Parquet", required = false)
  verify()
}

object Q4 {
    val log = Logger.getLogger(getClass().getName())

    val n_name = 1
    val c_custkey = 0
    val o_custkey = 1
    val o_orderkey = 0
    val l_orderkey = 0
    val c_nationkey = 3
    val n_nationkey = 0
    val l_shipdate = 10

  def main(argv: Array[String]) {
    val args = new Q4Conf(argv)
    val date_filter = args.date()
    val sc = SparkSession.builder
                    .appName("Q4")
                    .getOrCreate()
        
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val answer = if (args.parquet()) {
        val customer = sc.read.parquet(args.input() + "/customer")
                            .rdd
                            .map(pair => (pair.getInt(c_custkey), pair.getInt(c_nationkey)))

        val customer_tmp = sc.sparkContext.broadcast(customer.collectAsMap())

        val country = sc.read.parquet(args.input() + "/nation").rdd
                            .map(pair => (pair.getInt(n_nationkey), pair.getString(n_name)))

        val country_tmp = sc.sparkContext.broadcast(country.collectAsMap())
        sc.read.parquet(args.input() + "/lineitem")
                            .rdd
                            .filter(_.getString(l_shipdate).contains(date_filter))
                            .map(pair => (pair.getInt(l_orderkey), 1))
                            .reduceByKey(_ + _)      
                            .cogroup(sc.read.parquet(args.input() + "/orders")
                                            .rdd
                                            .map(pair => (pair.getInt(o_orderkey), pair.getInt(o_custkey))))                                                                 
                            .flatMap{ case(o_key, (count, o_tuple)) =>
                                        var pairs = scala.collection.mutable.ListBuffer[(Int, Int)]()
					                    var counts = count.iterator
					                    while (counts.hasNext) {
					 	                    pairs += ((customer_tmp.value(o_tuple.head), counts.next()));       
					                    }
					                pairs
                            }
                            .reduceByKey(_ + _)
                            .map{ case(nkey, count) => (nkey, (country_tmp.value(nkey), count))}
                            .sortByKey()
                            .collect()
                            .foreach(t => (println(t._1, t._2._1, t._2._2)))
    } else if (args.text()) {
        val customer = sc.sparkContext.textFile(args.input() + "/customer.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(c_custkey).toInt, pair(c_nationkey).toInt))

        val customer_tmp = sc.sparkContext.broadcast(customer.collectAsMap())

        val country = sc.sparkContext.textFile(args.input() + "/nation.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(n_nationkey).toInt, pair(n_name)))
        val country_tmp = sc.sparkContext.broadcast(country.collectAsMap())
        sc.sparkContext.textFile(args.input() + "/lineitem.tbl")
                            .map(_.split('|'))
                            .filter(_(l_shipdate).contains(date_filter))
                            .map(pair => (pair(l_orderkey).toInt, 1))
                            .reduceByKey(_ + _)   
                            .cogroup(sc.sparkContext.textFile(args.input() + "/orders.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(o_orderkey).toInt, pair(o_custkey).toInt)))                                                                 
                            .flatMap{ case(o_key, (count, o_tuple)) =>                                                                                 
                                var pairs = scala.collection.mutable.ListBuffer[(Int, Int)]()
					            var counts = count.iterator
					            while (counts.hasNext) {
					 	            pairs += ((customer_tmp.value(o_tuple.head), counts.next()));       
					            }
					            pairs
                            }
                            .reduceByKey(_ + _)
                            .map{ case(nkey, count) => (nkey, (country_tmp.value(nkey), count))}
                            .sortByKey()
                            .collect()
                            .foreach(t => (println(t._1, t._2._1, t._2._2)))
    }
  }
}