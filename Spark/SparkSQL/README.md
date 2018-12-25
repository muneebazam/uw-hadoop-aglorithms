# Spark SQL

The 7 scala programs here all implement diferrent SQL queries in a data warehousing scenario. The queries are performed on the Transaction Processing Performance Council (TPC-H) benchmark data. Information about it can be found here: *http://www.tpc.org/tpch/* Each program corresponds to the following queries:

*Q1.scala* -> How many items were shipped on a particular date? This corresponds to the following SQL query:

  `select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';`
