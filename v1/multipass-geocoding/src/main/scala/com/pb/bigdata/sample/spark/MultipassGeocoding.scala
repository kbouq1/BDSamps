package com.pb.bigdata.sample.spark

import com.mapinfo.geocode.{GeocodeAddress, GeocoderPreferences}
import com.mapinfo.geocode.api.StandardMatchMode
import com.pb.bigdata.geocoding.spark.api.GeocodeUDFBuilder
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, lit, map}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

object MultipassGeocoding {
  def main(args: Array[String]): Unit ={
    val inputAddressPath = args(0)
    val resourcesLocation = args(1)
    val downloadLocation = args(2)
    val outputDirectory = args(3)

    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Load the addresses from the csv
    val addressInputDF = AddressInput.open(session, inputAddressPath)

    // build a singleCandidateUDF, with custom GeocodeExecutor
    GeocodeUDFBuilder.singleCandidateUDFBuilder()
      .withGeocodeExecutor(new MultipassGeocodeExecutor())
      .withResourcesLocation(resourcesLocation)
      .withDownloadLocation(downloadLocation)
      .withOutputFields("X", "Y", "formattedStreetAddress", "formattedLocationAddress", "PrecisionCode")
      .withErrorField("error")
      .register("geocode", session)

    // call UDF for each row in the address DataFrame
    // this will result in the dataframe containing a new column for each of the specified
    // output fields as well as an error column
    addressInputDF
      // Adds a new column, represented as a collection comprised of the outputFields and the error field
      .withColumn("geocode_result", callUDF("geocode", map(
        lit("streetName"), col("address"),
        lit("postCode1"), col("postcode"),
        lit("areaName1"), col("state"),
        lit("areaName3"), col("city")))
      )
      // Persist the geocode result to avoid recalculation when we expand the result
      .persist()
      // Expand the result collection such that each output field is a separate column, including the error field.
      .select("*", "geocode_result.*").drop("geocode_result")
      // Write the dataframe to the specified output folder as parquet file
      .write.mode(SaveMode.Overwrite).parquet(outputDirectory)
  }
}

/*
 * A custom GeocodeExecutor that contains the logic to perform a multipass geocode
 */
class MultipassGeocodeExecutor extends com.pb.bigdata.geocoding.api.GeocodeExecutor {
  import com.mapinfo.geocode.api.{Address, GeocodeType, GeocodingAPI, Preferences, Response}

  var relaxedGeocoderPreferences: GeocoderPreferences = _

  @throws(classOf[com.mapinfo.geocode.GeocodingException])
  @Override
  def call(address:Address, preferences:Preferences, geocoder:GeocodingAPI) : Response  = {
    // Initialize a relaxed geocode preferences object
    if (relaxedGeocoderPreferences == null) {
      relaxedGeocoderPreferences = new GeocoderPreferences(preferences)
      relaxedGeocoderPreferences.setMatchMode(StandardMatchMode.RELAXED)
    }
    // perform a geocode of the address with no modification to address or preferences
    val response = geocoder.geocode(GeocodeType.ADDRESS, address, preferences)
    val candidates = response.getCandidates
    // If the candidate response has precisionCode and that precisionCode begins with a value
    // of S5 or greater, we are done, return the response
    if(CollectionUtils.isNotEmpty(candidates) && candidates.get(0).getPrecisionCode != null
      && Seq("S5","S6","S7","S8").contains(candidates.get(0).getPrecisionCode.slice(0,2))) {
      return response
    }
    // If the precisionCode is something less than an S5, turn the address in to a singleline
    // address
    val singleLine = address.getStreetName + " " +
      address.getAreaName3 + " " +
      address.getAreaName1 + " " +
      address.getPostCode1
    val singleLineAddress = new GeocodeAddress()
    singleLineAddress.setMainAddressLine(singleLine)

    //relax the "MatchMode" preference
    val singleLineResult = geocoder.geocode(GeocodeType.ADDRESS, singleLineAddress, relaxedGeocoderPreferences)

    // return the singleLine Result
    singleLineResult
  }
}

object AddressInput {
  // set these parameters according to the CSV
  def open(session: SparkSession, inputAddressPath: String): DataFrame = {
    session.read
      .option("delimiter", ",")
      .option("header", "false")
      .schema(inputAddressSchema)
      .csv(inputAddressPath)
  }

  private def inputAddressSchema: StructType = {
    // this is the schema for the address fabric data
    StructType(Array(
      StructField("address", StringType),
      StructField("city", StringType),
      StructField("state", StringType),
      StructField("postcode", StringType)
    ))
  }
}
