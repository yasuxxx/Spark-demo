package demo_mail
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object HotCaategoryTop10Analysis extends SparkDemoMain{
  override def process(spark: SparkSession): Unit = {
//    1.读取原始日志
    val actionRDD = spark.sparkContext.textFile("data/user_visit_action.txt")

//    2.统计点击数量 （品类id，点击数量）
    val clickActionRDD = actionRDD.filter(
      action=>{
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )



    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

//    val tmp = clickCountRDD.collect()

//    3.统计下单数量  （品类id，下单数量）
    val orderActionRDD = actionRDD.filter(
      action=>{
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id=>(id,1))
      }
    ).reduceByKey(_ + _)

//    4.统计支付数量  （品类id，支付数量）
    val payActionRDD = actionRDD.filter(
      action=>{
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id=>(id,1))
      }
    ).reduceByKey(_ + _)

//    5.将品类进行排序，并且取前十名  （点击-》下单-》支付）
    val corgroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
        clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val analysisRDD = corgroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCount = 0
        if (clickIter.iterator.hasNext) clickCount = clickIter.iterator.next()
        var orderCount = 0
        if (orderIter.iterator.hasNext) orderCount = orderIter.iterator.next()
        var payCount = 0
        if (payIter.iterator.hasNext) payCount = payIter.iterator.next()
        (clickCount, orderCount, payCount)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2,ascending = false).take(10)
//    6.将输出打印到控制台
    resultRDD.foreach(println)
  }
}
