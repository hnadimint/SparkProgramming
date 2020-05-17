import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkApp {
  def main(args: Array[String]): Unit = {
val conf = new SparkConf()
    conf.setAppName("FirstSparkApp").setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List.range(1,11),2)
    rdd.foreach(r => println(r))

    val rdd1 = sc.textFile("/home/cloudera/Desktop/sample.txt")
    rdd1.foreach(rec => println(rec))
  }
}
