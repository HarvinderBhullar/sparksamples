#Create keyspace and column family in cassandra

CREATE KEYSPACE MACHINEDATA
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
  
#Create table

CREATE TABLE moving_average (  machine_id text,  current  double,  current_alert double,  location text,  name text, state text, reading_time text, type text,  avg_current double, PRIMARY KEY (machine_id, reading_time) );

#Running spark job

bin\spark-submit --class  com.hs.movingaverage.SparkStreamMain --master local[4]  <your path>\target\moving-average-0.0.1-SNAPSHOT-jar-with-dependencies.jar