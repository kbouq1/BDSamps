![Pitney Bowes](PitneyBowes_Logo.jpg)

Geohash Aggregation Sample
---------------------
This sample demonstrates how to get geohashes for point locations and aggregate the data in each geohash. 
The locations in this sample come from social service requests into the NYC 311 information system.
Each request contains a NYC agency responsible for handling the request. 

## Description
The sample loads a CSV with longitudes and latitudes into a Spark DataFrame. 
Then a new column is added containing the geohash value for that location at a geohash precision of 7. 
The data is then grouped by a common geohash.
Each geohash record will have the total request count and the breakdown of the requests by NYC agency.

## Data
The sample includes the following data:

* **311data.csv**: Represents a subset of social service requests into the NYC 311 information system.
These files are from the [NYC Open Data website](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9). 
Each entry contains a NYC agency and the longitude and latitude coordinates that represent the location of the request. 
From its original form on the website, our sample has been filtered to include approximately 7 million incidents from five agencies:
  * **DOT**: Department of Transportation
  * **HPD**: Department of Housing Preservation and Development
  * **NYPD**: New York City Police Department
  * **DEP**: Department of Environmental Protection
  * **DSNY**: New York City Department of Sanitation
    
| Agency | Longitude  | Latitude  |
| :----- | ---------: | --------: |
|  DSNY  | -73.922369 | 40.670104 |
|  DSNY  | -73.922369 | 40.670104 |
|  NYPD  | -73.957987 | 40.713169 |

## Instructions
Download Spectrum Location Intelligence for Big Data and place the _spectrum-bigdata-li-sdk-spark2-&lt;version&gt;.jar_ into the /lib directory of this sample.

### Running the sample locally

The sample includes a gradle build system around it. 
To build the sample code, use the following command from the root of the sample:

    gradlew build

This command will compile the sample code, and will also execute the sample code via a JUnit test that is also included.
The test will run the sample locally using SparkSession and then do some simple JUnit asserts to verify the sample executed successfully. 
To only build the sample code without executing the test, you can exclude the test using the following command:

    gradlew build -x test

### Running the sample on a cluster

To execute the sample on a cluster, complete the following steps on your Hadoop cluster:
1. Copy the input data to a Hadoop cluster. This data is available in data directory of the sample.
1. Copy the input data to HDFS:
    ```
    hadoop fs -copyFromLocal <pathToSample>/geohash-aggregation/data/311data.csv /dir/on/hdfs/data
    ```
1. Create an output directory on HDFS:
    ```
    hadoop fs -mkdir /dir/on/hdfs/output
    ```    
1. Copy the _spectrum-bigdata-li-sdk-spark2-<version>.jar_ to a location on the cluster (/dir/on/cluster/lib)
1. Copy the output of the sample build _geohash-aggregation-1.0.0.jar_ to a location on the cluster (/dir/on/cluster/lib)
1. Execute a spark job on the cluster:
   ```sh
   spark-submit --class com.pb.bigdata.sample.spark.GeohashAggregation --master yarn --deploy-mode cluster --jars /dir/on/cluster/lib/spectrum-bigdata-li-sdk-spark2-<version>.jar /dir/on/cluster/lib/geohash-aggregation-1.0.0.jar hdfs:///dir/on/hdfs/data/311data.csv hdfs:///dir/on/hdfs/output
   ```

### Analyzing the output
The output will be a parquet table at the output location specified when running the spark-submit command. 
Use `spark.read.parquet("hdfs:///dir/on/hdfs/output")` to do further analysis on the table.

| Geohash | DEP| DOT|DSNY| HPD|NYPD|Count|polygon          |
| :-----  |---:|---:|---:|---:|---:| ---:| :-------------- |
| dr5nrxu |2| 6|5| |1|14|POLYGON ((-74.196...|
| dr5nwgm | | 1| | | | 1|POLYGON ((-74.227...|
| dr5nx4k |6|31|4| |2|43|POLYGON ((-74.218...|


## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows. 
The libraries were acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1. 
These libraries are referenced in the build.gradle file by adding the path to them as an environment variable to the included unit test. 
These libraries are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE
