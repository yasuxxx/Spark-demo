package demo_mail

import org.apache.spark.sql.SparkSession
import util.SparkUtil.getSparkSession
trait SparkDemoMain{
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession(appName)
    try {
      process(spark)
    }finally {
      spark.stop()
    }
  }
  def appName: String = this.getClass.getSimpleName
  def process(spark:SparkSession):Unit
}