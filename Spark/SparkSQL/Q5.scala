package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "Input Path", required = true)
  val text = opt[Boolean](descr = "Text", required = false)
  val parquet = opt[Boolean](descr = "Parquet", required = false)
  verify()
}

object Q5 {
    val log = Logger.getLogger(getClass().getName())

    val n_name = 1
    val c_custkey = 0
    val o_custkey = 1
    val c_nationkey = 3
    val n_nationkey = 0
    val o_orderkey = 0
    val l_orderkey = 0
    val canada_key = 3
    val states_key = 24
    val l_shipdate = 10

  def main(argv: Array[String]) {
    val args = new Q5Conf(argv)
    val sc = SparkSession.builder
                    .appName("Q5")
                    .getOrCreate()
        
    log.info("Input: " + args.input())

    val answer = if (args.parquet()) {
        val customer = sc.read.parquet(args.input() + "/customer").rdd
                            .map(pair => (pair.getInt(c_custkey), pair.getInt(c_nationkey)))
        val customer_temp = sc.sparkContext.broadcast(customer.collectAsMap())
        sc.read.parquet(args.input() + "/lineitem")
                .rdd
                .map(pair => (pair.getInt(l_orderkey), pair.getString(l_shipdate).substring(0, 7))) 
                .cogroup(sc.read.parquet(args.input() + "/orders").rdd
                            .map(pair => (pair.getInt(o_orderkey), pair.getInt(o_custkey)))
                            .filter{ case(o_key, ckey) =>
                                val nkey = customer_temp.value(ckey)
                                nkey == canada_key || nkey == states_key                        
                            }
                            .map{case(o_key, ckey) =>
                                (o_key, customer_temp.value(ckey))
                            })                                                                        
				.filter(_._2._2.size != 0)
				.flatMap{ case(o_key, (date, nkey)) =>
                    date.map(d => ((nkey.head, d), 1))
                }
                .reduceByKey(_ + _)
                .sortByKey()
                .collect()
                .foreach{ case((nkey, date), count) => (println(nkey, date, count))}
    } else if (args.text()) {
        val customer = sc.sparkContext.textFile(args.input() + "/customer.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(c_custkey).toInt, pair(c_nationkey).toInt))
        val customer_temp = sc.sparkContext.broadcast(customer.collectAsMap())
        sc.sparkContext.textFile(args.input() + "/lineitem.tbl")
                .map(_.split('|'))
                .map(pair => (pair(l_orderkey).toInt, pair(l_shipdate).substring(0, 7))) 
                .cogroup(sc.sparkContext.textFile(args.input() + "/orders.tbl")
                            .map(_.split('|'))
                            .map(pair => (pair(l_orderkey).toInt, pair(o_custkey).toInt))
                            .filter{case(_, ckey) =>
                                val nkey = customer_temp.value(ckey)
                                nkey == canada_key || nkey == states_key                        
                            }
                            .map{ case(o_key, ckey) => 
                                (o_key, customer_temp.value(ckey).toInt)
                            })                                                                        
				.filter(_._2._2.size != 0)
				.flatMap{ case(o_key, (date, nkey)) =>
					var nationkey = nkey.head
                	date.map(d => ((nationkey, d), 1))
				}
                .reduceByKey(_ + _)
                .sortByKey()
                .collect()
                .foreach{ case((nkey, date), count) => (println(nkey, date, count))}
    }
  }
}