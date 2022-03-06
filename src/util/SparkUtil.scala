package util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.Map

object SparkUtil {
  def getSparkSession(name: String, attrs: Map[String, String] = Map.empty): SparkSession = {
    val conf = new SparkConf().setAppName(name).setIfMissing("spark.master", "local[2]")
    attrs.foreach(e => conf.set(e._1, e._2))
    SparkSession.builder().config(conf).getOrCreate()//.enableHiveSupport()
  }
  def getSparkContext(name: String, attrs: Map[String, String] = Map.empty): SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[2]")
    sparkConf.setAppName(name)
    attrs.foreach(e => sparkConf.set(e._1, e._2))
    new SparkContext(sparkConf)
  }

  def getSparkConf(name: String, attrs: Map[String, String] = Map.empty): SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[2]")
    sparkConf.setAppName(name)
    attrs.foreach(e => sparkConf.set(e._1, e._2))
    sparkConf
  }
}

