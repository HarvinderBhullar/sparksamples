# sparksamples - Moving avererage

###Pre-requisite

* Apache Spark 1.5.2
* Cassandra 2.x Community Edition

###Approach

Machine data is available from public API http://machinepark.actyx.io/api/v1

*  Create CustomHttpReceiver, which will create a stream of JSON data
*  Append the machine id to the returned JSON data
*  Create stream in Apache Spark by specifying sliding interval
*  Use a sliding window feature of Apache spark, by specifying window interval
*  Use the SQL DataFrame API to check for alarm and finding moving average
*  Store the data in Casssandra

*In production, post the data from CustomHttpReceiver on to a Kafka and then write separate job for analysis*



####Create keyspace and column family in cassandra

    CREATE KEYSPACE MACHINEDATA WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
  
####Create table

    CREATE TABLE moving_average (  machine_id text,  current  double,  current_alert double,  location text,  name text, state text, reading_time text, type text,  avg_current double, PRIMARY KEY (machine_id, reading_time) );

####Running spark job

    bin\spark-submit --class  com.hs.movingaverage.SparkStreamMain --master local[4]  <your path>\target\moving-average-0.0.1-SNAPSHOT-jar-with-dependencies.jar
