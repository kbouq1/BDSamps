package com.pb.bigdata.sample.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object VerificationDriver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val resultDataPath = args(0)

    val resultDF = session.read.parquet(resultDataPath)

    import org.scalatest.Assertions._
    // confirm count of result dataframe
    assert(resultDF.count == 200)

    // confirm locationAddress contains city, State
    assert(resultDF.filter(resultDF("formattedLocationAddress").contains("WASHINGTON, DC")).count() === 200)

    // confirm no errors
    assert(resultDF.filter(resultDF("error").isNotNull).count() === 0)
  }
}
