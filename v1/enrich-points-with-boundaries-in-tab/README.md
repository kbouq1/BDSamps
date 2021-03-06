![Pitney Bowes](PitneyBowes_Logo.jpg)

Tab Enrichment Sample
---------------------
This sample demonstrates how to use Spark to enrich a CSV containing point data with attributes from a TAB file based on 
a point in polygon search.  Enriching your point data with another dataset can provide you with 
additional context. For example, let's say that you have a list of clients represented by the Address Fabric
sample below. Understanding what risks are associated with those clients can help you price products such as insurance. 
This sample demonstrates joining the Address Fabric to the Crime Index data.

## Description
The sample loads an Address Fabric CSV with longitudes and latitudes into a Spark DataFrame. In the sample, 
columns from the crime TAB data are added based on a point in polygon search of the crime TAB data. 

## Data
This sample includes data from Pitney Bowes [Software and Data Marketplace (SDM)](https://www.pitneybowes.com/us/data/sdm.html). 

* **us_address_fabric_san_francisco.txt**
Sample of points from Pitney Bowes [Address Fabric Data](https://www.pitneybowes.com/us/data/addressing-data/geocoded-data.html).

* **Crime Data**
Sample crime data from Pitney Bowes [Crime Data](https://www.pitneybowes.com/us/data/boundary-data/crime-data.html).

By downloading the sample data, you are agreeing to a 30-day trial and our [Evaluation Terms](https://www.pitneybowes.com/content/dam/pitneybowes/us/en/license-terms-of-use/trial-evaluation-agreement-terms---u-s-/trial-evaluation-agreement-url-us-may2017.pdf).

## Instructions
Download Spectrum Location Intelligence for Big Data and place the _spectrum-bigdata-li-sdk-spark2-<version>.jar_ into the /lib directory of this sample.

### Running the sample locally

The sample includes a gradle build system around it.  To build the sample code, use the following command from the root of the sample:

    gradlew build

This command will compile the sample code, execute the sample code via a JUnit test that is also included, and generate 
a single fat jar that contains all the required code.  The test will run the sample locally using SparkSession and then 
do some simple JUnit asserts to verify the sample executed successfully.  To only build the sample code without 
executing the test, you can exclude the test using the following command:

    gradlew build -x test

### Running the sample on a cluster

To execute the sample on a cluster, complete the following steps on your Hadoop cluster:
1. Copy the input data to a Hadoop cluster. This data is available in the data directory of the sample.
1. Copy the input data to HDFS:
     ```
     hadoop fs -copyFromLocal <pathToSample>/enrich-points-with-boundaries-in-tab/data/us_address_fabric_san_francisco.txt /dir/on/hdfs/data/input
     hadoop fs -copyFromLocal <pathToSample>/enrich-points-with-boundaries-in-tab/data/CrimeIndex_Sample/tab /dir/on/hdfs/data/
     ```
1. Create an output directory on HDFS:
      ```
      hadoop fs -mkdir /dir/on/hdfs/output
      ```
1. Copy the _spectrum-bigdata-li-sdk-spark2-<version>.jar_ to a location on the cluster (/dir/on/cluster/lib)
1. Copy the output of the sample build _enrich-points-with-boundaries-in-tab-1.0.0.jar_ to a location on the cluster (/dir/on/cluster/lib)
1. Determine or create a local directory that is available on all nodes in the cluster (/local/download/dir) This 
directory is used to locally cache the TAB data for use during the search.
1. Execute a spark job on the cluster:
   ```sh
   spark-submit --class com.pb.bigdata.sample.spark.TABEnrichment --master yarn --deploy-mode cluster --jars /dir/on/cluster/lib/spectrum-bigdata-li-sdk-spark2-<version>.jar /dir/on/cluster/lib/enrich-points-with-boundaries-in-tab-1.0.0.jar hdfs:///dir/on/hdfs/data/CrimeIndex_Sample/tab hdfs:///dir/on/hdfs/data/us_address_fabric_san_francisco.txt hdfs:///dir/on/hdfs/output /local/download/dir
    ```

## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows.  The libraries were 
acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1.  These libraries are referenced in 
the build.gradle file by adding the path to them as an environment variable to the included unit test.  These libraries 
are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE

