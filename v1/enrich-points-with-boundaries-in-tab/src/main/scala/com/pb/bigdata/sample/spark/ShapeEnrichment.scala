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

object ShapeEnrichment {

  def main(args: Array[String]): Unit = {
    val shapeDirectory = args(0)
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

    val table = TableBuilder.ShapeTable(shapeDirectory, "CrimeIndex_2019_Sample_region.shp").withDownloadManager(downloadManager).build()

    val contains = (x : Double, y: Double) => {
      val selectList = new SelectList (
        "c_crime_i", // composite crime index
        "v_crime_i", // violent crime index
        "assault_i", // aggrevated assault index
        "p_crime_i", // property crime index
        "arson_i", // arson index
        "burglary_i", // burglary index
        "a_theft_i", // auto theft index
        "c_crime_c", // composite crime category
        "v_crime_c", // violent crime category
        "assault_c", // aggrevated assault category
        "p_crime_c", // property crime category
        "arson_c", // arson category
        "burglary_c", // burglary category
        "a_theft_c" // auto theft category
      )
      val point = new Point(SpatialInfo.create(CoordSysConstants.longLatWGS84), new DirectPosition(x, y))
      // create the point-in-polygon filter
      val filter = new GeometryFilter("the_geom", GeometryOperator.CONTAINS, point)
      // create a search for all the specified columns and the specified filter
      val filterSearch = new FilterSearch(selectList, filter, null)
      table.search(filterSearch)
    }

    spark.read.option("delimiter", "\t").option("header", "true").csv(addressFabricPath)
      .select("PBKEY", "LON", "LAT")
      .withSpatialSearchColumns(Seq(col("LON").cast(DoubleType), col("LAT").cast(DoubleType)), contains, includeEmptySearchResults = false)
      .write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}
