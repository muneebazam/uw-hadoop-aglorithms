# Spark SQL

The 7 scala programs here all implement diferrent SQL queries in a data warehousing scenario. The queries are performed on the Transaction Processing Performance Council (TPC-H) benchmark data. Information about it can be found here: *http://www.tpc.org/tpch/* Each program corresponds to the following queries:

*Q1.scala* -> How many items were shipped on a particular date? This corresponds to the following SQL query:

`select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';`

*Q2.scala* -> Which clerks were responsible for processing items that were shipped on a particular date? List the first 20 by order key. This corresponds to the following SQL query:

`select o_clerk, o_orderkey from lineitem, orders\
where\
  l_orderkey = o_orderkey and\
  l_shipdate = 'YYYY-MM-DD'\
order by o_orderkey asc limit 20;`

*Q3.scala* -> What are the names of parts and suppliers of items shipped on a particular date? List the first 20 by order key. This corresponds to the following SQL query:

`select l_orderkey, p_name, s_name from lineitem, part, supplier
where
  l_partkey = p_partkey and
  l_suppkey = s_suppkey and
  l_shipdate = 'YYYY-MM-DD'
order by l_orderkey asc limit 20;`

*Q4.scala* -> How many items were shipped to each country on a particular date? This corresponds to the following SQL query:

`select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
where
  l_orderkey = o_orderkey and
  o_custkey = c_custkey and
  c_nationkey = n_nationkey and
  l_shipdate = 'YYYY-MM-DD'
group by n_nationkey, n_name
order by n_nationkey asc;`

*Q5.scala* -> This query represents a very simple end-to-end ad hoc analysis task: Related to Q4, your boss has asked you to compare shipments to Canada vs. the United States by month, given all the data in the data warehouse. You think this request is best fulfilled by a line graph, with two lines (one representing the US and one representing Canada), where the x-axis is the year/month and the y axis is the volume, i.e., count(*). Generate this graph for your boss.

*Q6.scala* -> This is a slightly modified version of TPC-H Q1 "Pricing Summary Report Query". This query reports the amount of business that was billed, shipped, and returned:

`select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from lineitem
where
  l_shipdate = 'YYYY-MM-DD'
group by l_returnflag, l_linestatus;`

*Q7.scala* -> This is a slightly modified version of TPC-H Q3 "Shipping Priority Query". This query retrieves the 10 unshipped orders with the highest value:

`select
  c_name,
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from customer, orders, lineitem
where
  c_custkey = o_custkey and
  l_orderkey = o_orderkey and
  o_orderdate < "YYYY-MM-DD" and
  l_shipdate > "YYYY-MM-DD"
group by
  c_name,
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc
limit 10;`

  
  
