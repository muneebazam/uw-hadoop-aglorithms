package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "Input Path", required = true)
  val date = opt[String](descr = "Date", required = true)
  val text = opt[Boolean](descr = "Text", required = false)
  val parquet = opt[Boolean](descr = "Parquet", required = false)
  verify()
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  val l_quantity = 4
  val l_ext_price = 5
  val l_discount = 6
  val l_tax = 7
  val l_return = 8
  val l_status = 9
  val l_ship_date = 10

  def main(argv: Array[String]) {
    val args = new Q6Conf(argv)
    val date_filter = args.date()
    val sc = SparkSession.builder
                    .appName("Q6")
                    .getOrCreate()
        
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    if (args.text()) {
      sc.sparkContext.textFile(args.input() + "/lineitem.tbl")
                      .map(_.split('|'))
                      .filter(_(l_ship_date).contains(date_filter))
                      .map(item =>
							       ((item(l_return),
                                     item(l_status)),
                                     (item(l_quantity).toDouble,
                                      item(l_ext_price).toDouble,
                                      item(l_ext_price).toDouble * (1 - item(l_discount).toDouble),
                                      item(l_ext_price).toDouble * (1 - item(l_discount).toDouble) * (1 + item(l_tax).toDouble),
                                      item(l_discount).toDouble,
                                      1)))
                        .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
                        .map{ case(((return_flag, status),
                                    (sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_discount, count)))
                                    => {
									                    	((return_flag, status),
                                        (sum_qty, sum_base_price, sum_disc_price, sum_charge,
                                        sum_qty/count, sum_base_price/count, sum_discount/count,
                                        count))}
                        }
                        .sortByKey()
                        .collect()
                        .foreach{ case(((return_flag, status),
                                        (sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_base_price, avg_discount, count)))
                                        => println(return_flag, status,
                                                  sum_qty, sum_base_price, sum_disc_price, sum_charge,
                                        avg_qty, avg_base_price, avg_discount, count)
                        }
    } else if (args.parquet()) {
      sc.read.parquet(args.input() + "/lineitem")
              .rdd
              .filter(_.getString(l_ship_date).contains(date_filter))
              .map(item =>
                            ((item.getString(l_return),
                              item.getString(l_status)),
                              (item.getDouble(l_quantity),
                              item.getDouble(l_ext_price),
                              item.getDouble(l_ext_price) * (1 - item.getDouble(l_discount)),
                              item.getDouble(l_ext_price) * (1 - item.getDouble(l_discount)) * (1 + item.getDouble(l_tax)),
                              item.getDouble(l_discount),
                              1)))
                .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
                .map{ case(((return_flag, status),
                            (sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_discount, count)))
                            => {
                                ((return_flag, status),
                                (sum_qty, sum_base_price, sum_disc_price, sum_charge,
                                sum_qty/count, sum_base_price/count, sum_discount/count,
                                count))}
                }
                .sortByKey()
                .collect()
                .foreach{ case(((return_flag, status),
                                (sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_base_price, avg_discount, count)))
                                => println(return_flag, status,
                                          sum_qty, sum_base_price, sum_disc_price, sum_charge,
                                avg_qty, avg_base_price, avg_discount, count)
                }
    }
  }
}