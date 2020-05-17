package aggregate

import org.apache.spark.{SparkConf, SparkContext}
object AmountWiseDiscount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)
      //.setMaster("local")

    val sc = new SparkContext(conf)
    val salesRDD = sc.textFile(args(0))
    salesRDD.foreach(r => println(r))

    val salesPairRDD = salesRDD.map(rec => {val fieldArr = rec.split(",")
      (fieldArr(1).toInt,fieldArr(3).toDouble)})
    salesPairRDD.foreach(r => println(r))

    val totalAmountRDD = salesPairRDD.reduceByKey(_+_)

    println(totalAmountRDD.collect.toList)

    Thread.sleep(300000)

  }

}
