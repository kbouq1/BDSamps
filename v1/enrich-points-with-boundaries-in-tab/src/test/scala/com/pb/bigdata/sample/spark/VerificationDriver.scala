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
    val testColumn = args(1)

    val resultDF = session.read.parquet(resultDataPath)

    //spot check some results
    import org.scalatest.Assertions._
    assert(resultDF.count == 548307)

    val sampleRecord = resultDF.filter(resultDF("PBKEY") === "P00002T1QGHL").first()
    assert(sampleRecord.getString(resultDF.schema.fieldIndex(testColumn)) === "High")
  }
}
