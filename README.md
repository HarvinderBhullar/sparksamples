# Sparksamples 

* Moving Average
* Data Collection - (machine and environment data)
* Correlation between machine data and environment data

###Pre-requisite

* Apache Spark 1.5.2
* Cassandra 2.x Community Edition

###Approach - Moving Average

Machine data is available from public API http://machinepark.actyx.io/api/v1

*  Create CustomHttpReceiver, which will create a stream of JSON data
*  Append the machine id to the returned JSON data
*  Create stream in Apache Spark by specifying sliding interval
*  Use a sliding window feature of Apache spark, by specifying window interval
*  Use the SQL DataFrame API to check for alarm and finding moving average
*  Store the data in Casssandra

*In production, post the data from CustomHttpReceiver on to a Kafka and then write separate job for analysis*

###Approach - Data Collection

*  Use CustomHttpReceiver, which will create a stream of JSON data
*  Append the machine id to the returned JSON data
*  Create CustomHttpEnvReceiverEnv to get Environment Data
*  Store the data in Casssandra

###Approach - Correlating Machine Data with Environment Data

*  Export the data from Cassandra collected in the Data Collection stage
*  It is necessary to extract the data in CSV format so that others can verify thier model with the same data set
*  Since there are not many dimension in this case, finding a pair wise correlation itself will provide the required insight.

