# Spark Streaming

Utilizing the Spark Streaming framework to provide insight on taxi trips taken in New York City. Data for over one billion taxi trips made over the past several years in New York has been provided by the New York City Taxi & Limousine Commission. The data can be found here: *http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml* Although this data is historic it goes through multiple contortions to be treated as a stream of data on which we can apply Streaming aggregations.

The *EventCount.scala* program aggregates number of individual trips made in the city by the hour.

The *RegionEventCount.scala* program aggregates the number of individual trips to either the Goldman Sachs or Citigroup headquarters by the hour.

The *TrendingArrivals.scala* program aggregates the number of trips made to either the Goldman Sachs or Citigroup headquarters every 10 minutes and prints a notification on stdout whenever an interval experiences a 2x increase in number of trips compared to its preceding interval. 
