package com.pb.bigdata.sample.spark

import java.util.logging.{ConsoleHandler, Level, Logger}

import org.apache.commons.lang.SystemUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class MultipassGeocodingTester extends FunSuite with BeforeAndAfterAll {

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
    val paths = new Array[String](5)

    paths(0) = "./data/addresses.csv"
    paths(1) = "./resources"
    paths(2) = "./build/download"
    paths(3) = "./build/output"

    // Execute our Multipass Geocoding sample
    MultipassGeocoding.main(paths)

    //verify the results of the sample run
    VerificationDriver.main(Array(paths(3)))
  }
}
