package aggregate

import org.apache.spark.{SparkConf, SparkContext}

object ItemWiseDiscount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)
    val salesRDD = sc.textFile(args(0))
    salesRDD.foreach(r => println(r))

    val salesPairRDD = salesRDD.map(rec => {
      val fieldArr = rec.split(",")
      (fieldArr(1).toInt, fieldArr(3).toDouble)
    })
    salesPairRDD.foreach(r => println(r))
    val DiscountRDD = salesPairRDD.map(t => {
      val amount =
        0.95 * t._2
      (t._1, amount)
    })
    DiscountRDD.foreach(r => println(r))
    val totalAmountRDD = DiscountRDD.reduceByKey(_ + _)

    totalAmountRDD.foreach(r => println(r))


  }
}
