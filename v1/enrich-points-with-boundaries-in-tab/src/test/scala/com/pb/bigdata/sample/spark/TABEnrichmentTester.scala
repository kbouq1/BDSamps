package com.pb.bigdata.sample.spark

import java.util.logging.{ConsoleHandler, Level, Logger}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class TABEnrichmentTester extends FunSuite with BeforeAndAfterAll {
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

  test("Execute tab sample and assert output") {
    testDriver("tab", args => TABEnrichment.main(args))
  }

  test("Execute tab sample with schema and assert output") {
    testDriver("tab", args => TABEnrichmentWithSchema.main(args))
  }

  test("Execute shape sample and assert output") {
    testDriver("shape", args => ShapeEnrichment.main(args))
  }

  def testDriver(format : String, mainMethodExecution : Array[String] => Unit) : Unit = {
    val paths = new Array[String](4)

    paths(0) = "./data/CrimeIndex_Sample/" + format
    paths(1) = "./data/us_address_fabric_san_francisco.txt"
    paths(2) = "./build/output"
    paths(3) = "./build/downloadManager"

    // Execute our Point Enrichment sample
    mainMethodExecution.apply(paths)

    //verify the results of the sample run
    val testColumn = if(format.equals("tab")) "auto_theft_category" else "a_theft_c"
    VerificationDriver.main(Array(paths(2), testColumn))
  }
}
