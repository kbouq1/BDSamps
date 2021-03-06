package com.pb.bigdata.sample.spark

import com.mapinfo.midev.coordsys.CoordSysConstants
import com.mapinfo.midev.geometry.impl.Point
import com.mapinfo.midev.geometry.{DirectPosition, SpatialInfo}
import com.mapinfo.midev.language.filter.{FilterSearch, GeometryFilter, GeometryOperator, SelectList}
import com.pb.bigdata.li.spark.api.SpatialImplicits._
import com.pb.bigdata.li.spark.api.table.TableBuilder
import com.pb.downloadmanager.api.DownloadManagerBuilder
import com.pb.downloadmanager.api.downloaders.LocalFilePassthroughDownloader
import com.pb.downloadmanager.api.downloaders.hadoop.{HDFSDownloader, S3Downloader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

object TABEnrichmentWithSchema {

  def main(args: Array[String]): Unit = {
    val tabDirectory = args(0)
    val addressFabricPath = args(1)
    val outputPath = args(2)
    val downloadLocation = args(3)

    val sparkConf = new SparkConf().setIfMissing("spark.master", "local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val downloadManager = new DownloadManagerBuilder(downloadLocation)
      .addDownloader(new HDFSDownloader(spark.sparkContext.hadoopConfiguration))
      .addDownloader(new S3Downloader(spark.sparkContext.hadoopConfiguration))
      .addDownloader(new LocalFilePassthroughDownloader())
      .build()

    val table = TableBuilder.NativeTable(tabDirectory, "CrimeIndex_2019_Sample.TAB").withDownloadManager(downloadManager).build()

    val tableColumns = Seq(
      "composite_crime_idx",
      "violent_crime_idx",
      "aggravated_assault_idx",
      "property_crime_idx",
      "arson_idx",
      "burglary_idx",
      "auto_theft_idx",
      "composite_crime_category",
      "violent_crime_category",
      "aggravated_assault_category",
      "property_crime_category",
      "arson_category",
      "burglary_category",
      "auto_theft_category")

    val searchSchema = table.getMetadata.getAttributeDefinitions
      .filter(attr => tableColumns.contains(attr.getName))
      .map(_.toStructField())

    val contains = (x : Double, y: Double) => {
      val selectList = new SelectList (tableColumns : _*)
      val point = new Point(SpatialInfo.create(CoordSysConstants.longLatWGS84), new DirectPosition(x, y))
      // create the point-in-polygon filter
      val filter = new GeometryFilter("OBJ", GeometryOperator.CONTAINS, point)
      // create a search for all the specified columns and the specified filter
      val filterSearch = new FilterSearch(selectList, filter, null)
      table.search(filterSearch)
    }

    spark.read.option("delimiter", "\t").option("header", "true").csv(addressFabricPath)
      .select("PBKEY", "LON", "LAT")
      .withSpatialSearchColumns(Seq(col("LON").cast(DoubleType), col("LAT").cast(DoubleType)), contains, includeEmptySearchResults = false, schema = searchSchema)
      .write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}
