package com.pb.bigdata.sample.spark

import java.util.logging.{ConsoleHandler, Level, Logger}

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.Assert.{assertFalse, assertTrue}

@RunWith(classOf[JUnitRunner])
class PointEnrichmentTester extends FunSuite with BeforeAndAfterAll {
  private val parquetLogger = Logger.getLogger("org.apache.parquet") //holding on to logger to persist log level changes

  override def beforeAll() {
    //turn down the logging for spark logs which uses log4j
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.ERROR) //change log level if more debug information is needed
    org.apache.log4j.Logger.getRootLogger.addAppender(new org.apache.log4j.ConsoleAppender())

    //turn down the logging for the parquet package which uses java.util.logging
    val handler = new ConsoleHandler
    handler.setLevel(Level.SEVERE)
    parquetLogger.addHandler(handler)
  }

  test("Execute sample and assert output") {
    val paths = new Array[String](3)

    paths(0) = "./data/WPPOI_USA.txt"
    paths(1) = "./data/us_address_fabric_san_francisco.txt"
    paths(2) = "./build/output"

    // Execute our Point Enrichment sample
    PointEnrichment.main(paths)

    //verify the results of the sample run
    VerificationDriver.main(Array(paths(2)))
  }
}
