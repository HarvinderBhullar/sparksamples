### Data Collection

### Create Table for for Env Data

    CREATE TABLE env_data (  readtime text,  pressure  double,  temperature double,  humidity double, PRIMARY KEY (readtime) );

### Create Table for Collection Machine Data

    CREATE TABLE machine_data (  machine_id text,  current  double,  current_alert double,  location text,  name text, state text, reading_time text, type text,  PRIMARY KEY (machine_id, reading_time) );


### Running the Spark job

    bin\spark-submit --class  com.hs.datacollection.SparkStreamDataCollectionMain --master local[4]  C:\Users\Sukhleen\workspace\data-collection\target\data-collection-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    
     bin\spark-submit --class  com.hs.datacollection.SparkStreamDataCollectionEnvMain --master local[4]  C:\Users\Sukhleen\workspace\data-collection\target\data-collection-0.0.1-SNAPSHOT-jar-with-dependencies.jar