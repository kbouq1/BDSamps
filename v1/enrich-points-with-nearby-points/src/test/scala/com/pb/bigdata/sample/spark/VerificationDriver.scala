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
    assert(!resultDF.head(1).isEmpty)

    // confirm count of result dataframe
    assert(resultDF.count == 91905819)

    assert(resultDF.filter(resultDF("NAME") === "WOODEN WINDOW").count == 85)
    assert(resultDF.filter(resultDF("NAME") === "WOODEN WINDOW" && resultDF("PBKEY") === "P00002T1UDQS").count == 1)
  }
}
