![Pitney Bowes](PitneyBowes_Logo.jpg)

Multipass Geocoding Sample
---------------------
This sample for the Spark Geocoding API in Scala demonstrates how to improve geocoding results by performing multipass geocoding.  With multipass geocoding, for all first-pass results with suboptimal precision levels, i.e., any result code less than S5 (street interpolated), a second geocoding pass is run using single line address matching, which may return more accurate geocodes. The sample includes a list of input addresses and geocoding reference data. 

## Description
The sample implements a custom `com.pb.bigdata.geocoding.api.GeocodeExecutor` from the Spark Geocoding API.  This custom `GeocodeExecutor` contains the logic to perform the multipass geocode.  The class is then supplied to the `GeocodeUDFBuilder` along with all the other necessary GGS configurations.  Using a spark.sql Dataframe, a csv of addresses is created. Then using a `withColumn` method, the `GeocodeUDFBuilder` UDF is called to perform the geocode on the input address.  The result will be columns added to the dataframe with the results of the geocode.

## Data
The sample includes the following data located in the `/data` folder:
* **addresses.csv**: Sample list of Washington, DC addresses.
* **WashingtonDCReferenceData**: Folder containing the sample geocoding reference data for Washington, DC.  

## Setup instructions for running locally
1. Download the Spectrum Geocoding for Big Data distribution and extract the contents.
2. Place the _spectrum-bigdata-geocoding-sdk-spark2-<version>.jar_ into the `/lib` directory of this sample.
3. Place the contents of the `/resources` directory into the `/resources` directory of this sample.
4. From the sample's `/resources` directory, copy and replace the _JsonDataConfig.json_ file found in the `/resources/config` directory of this sample.

### Running the sample locally

The sample includes a gradle build system around it.  To build the sample code, use the following command from the root of the sample:

    gradlew build

This command will compile the sample code, execute the sample code via a JUnit test that is also included, and generate 
a jar that contains all the compiled sample code.  The test will run the sample locally using SparkSession, and then 
do some simple JUnit asserts to verify the sample executed successfully.  To only build the sample code without 
executing the test, you can exclude the test using the following command:

    gradlew build -x test

### Running the sample on a cluster 
** This procedure assumes you have already built the sample and ran it locally.

To execute the sample on a cluster, complete the following steps on your Hadoop cluster:
1. Install the geocoding reference data:
    - Create the install directory.
    ```sh
    mkdir /sample
    ```
    - Add the geocoding distribution zip to the node at a temporary location, for example:
    ```sh
    /sample/temp/spectrum-bigdata-geocoding-<version>.zip
    ```
    - Extract the Geocoding distribution.
    ```sh
    mkdir /sample/geocoding
    mkdir /sample/geocoding/software
    unzip /sample/temp/spectrum-bigdata-geocoding-<version>.zip -d /sample/geocoding/software
    ```
    - Copy the geocoding reference data to the cluster in a new data folder.  This data can be found as part of this sample in the `./data/WashingtonDCReferenceData` folder.
    ```sh
    mkdir /sample/geocoding
    mkdir /sample/geocoding/data
    ```
    ```sh
    /sample/geocoding/data/WashingtonDCReferenceData
    ```
    - Navigate to the Geocoding CLI.
    ```sh
    cd /sample/geocoding/software/cli
    ```
    - Using the Geocoding CLI tool, configure the geocoding reference data.
    ```sh
    bash cli.sh configure --s /sample/geocoding/data/WashingtonDCReferenceData --d /sample/geocoding/software/resources/config
    ```
2. Distribute the reference data using HDFS:
    - Create an install directory on HDFS.
    ```sh
    hadoop fs -mkdir /sample
    ```
    - Upload the reference data into HDFS.
    ```sh
    hadoop fs -mkdir /sample/geocoding
    hadoop fs -mkdir /sample/geocoding/data
    hadoop fs -copyFromLocal /sample/geocoding/data/WashingtonDCReferenceData hdfs:///sample/geocoding/data/WashingtonDCReferenceData
    ```
    - Configure an HDFS root.
    ```sh
    bash cli.sh setting --t "ROOTS" --n "root1" --v "hdfs:///sample/geocoding/data/WashingtonDCReferenceData" --d /sample/geocoding/software/resources/config
    ```
    - Upload the resources folder into HDFS.
    ```sh
    hadoop fs -mkdir /sample/geocoding/software
    hadoop fs -copyFromLocal /sample/geocoding/software/resources hdfs:///sample/geocoding/software/resources
    ```
3. Copy the sample addresses to HDFS or S3:
    - Copy the sample _addresses.csv_ file to HDFS or S3. This file can be found in the `./data` folder of the sample. 
    - Copy `./data/addresses.csv` to a temporary location on a node, such as: 
    ```sh
    mkdir /sample/temp/DC
    ```
    - Create a location on HDFS or S3, and copy _addresses.csv_ to it, for example:
    ```sh
    hadoop fs -mkdir /sample/geocoding/sampleData
    hadoop fs -mkdir /sample/geocoding/sampleData/DC
    ```
    ```sh
    hadoop fs -copyFromLocal /sample/temp/DC/addresses.csv hdfs:///sample/geocoding/sampleData/DC
    ```
4. Copy the output of the sample build _multipass-geocoding-<version>.jar_ to a location on the cluster, such as:
    ```sh
    mkdir /sample/build
    ```
5. Determine or create a local directory that is available on all nodes in the cluster (`/local/download/dir`).   This directory is used by Download Manager to distribute data to all nodes and must exist and be accessible on every node.

6. Execute a spark job on the cluster.
   ```sh
   spark-submit --class com.pb.bigdata.sample.spark.MultipassGeocoding --master yarn --deploy-mode cluster --jars /sample/geocoding/software/spark2/sdk/lib/spectrum-bigdata-geocoding-sdk-spark2-<version>.jar /sample/build/multipass-geocoding-<version>.jar hdfs:///sample/geocoding/sampleData/DC/addresses.csv hdfs:///sample/geocoding/software/resources /local/download/dir hdfs:///sample/output
    ```

## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows.  The libraries were 
acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1.  These libraries are referenced in 
the build.gradle file by adding the path to them as an environment variable to the included unit test.  These libraries 
are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE