package com.pb.bigdata.sample.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.scalatest.Assertions._

object VerificationDriver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val resultDataPath = args(0)

    val df = session.read.parquet(resultDataPath)

    assert(df.count() == 33926)

    val nypdMax = df.agg(max(df("NYPD"))).first()
    assert(nypdMax.getLong(0) == 1453)
  }
}
