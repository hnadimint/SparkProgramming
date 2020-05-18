package aggregate.caching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

case class SalesRecord(transactionid: Int, customerid: Int,
                 itemid: Int, itemprice: Double)

object ItemWiseDiscount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setMaster("local")

    val sc = new SparkContext(conf)

    val fileRDD = sc.textFile(args(0))

    // impose schema/structure on the raw data using a case class
    val salesRDD = fileRDD.map(rec => {
      val fieldArr = rec.split(",")
      val sr = SalesRecord(fieldArr(0).toInt, fieldArr(1).toInt, fieldArr(2).toInt, fieldArr(3).toDouble)
      sr
    })

    /*salesRDD.foreach(sr => println(sr.customerid,sr.itemprice))
    salesRDD.foreach(sr => println(sr.customerid,sr.itemid))
    salesRDD.foreach(sr => println(sr.customerid,sr.transactionid))*/
    val salesPairRDD = salesRDD.map(sr => (sr.customerid, sr.itemprice * 0.95))

    val totalAmountRDD = salesPairRDD.reduceByKey(_ + _)
    totalAmountRDD.persist(StorageLevel.MEMORY_ONLY_SER)

    println(totalAmountRDD.collect.toList)
    totalAmountRDD.foreach(println)

    // flush off from the memory (or) clear RDD from the cache memory
    totalAmountRDD.unpersist()
    salesPairRDD.cache
    Thread.sleep(300000)
  }

}
