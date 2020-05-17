package intro

import org.apache.spark.{SparkConf, SparkContext}
object WordCountAppScala {
  def main(args: Array[String]): Unit = {
    //app name as intro WordCountApp
    val conf = new SparkConf().setAppName(getClass.getName)
        .setMaster("local")
    val sc = new SparkContext(conf)

    //read the text file//

    val fileRDD = sc.textFile(args(0))
    //extract every word as a seperate word,splitting by space and flatting it//
    val wordRDD = fileRDD.flatMap(line => line.split(" "))

    wordRDD.foreach(r => println(r))

    //prepare data in the form (k,v) pairs for aggregation to apply//
    val wordspairRDD = wordRDD.map(word => (word,1))

    wordspairRDD.foreach(r => println(r))
    //apply aggregation using reduceByKey instead of groupKey //
    val wordCountRDD = wordspairRDD.reduceByKey((acc,itr) => acc + itr)
    wordspairRDD.foreach(r => println(r))

    wordspairRDD.reduceByKey(_+_)
    println(wordCountRDD.collect.toList)

  }
}
