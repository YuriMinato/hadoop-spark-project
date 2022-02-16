import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val all = textFile.flatMap(line => line.split(",")).filter(_.trim != "").map(word => (word, 1 )).count()
    val num = textFile.count()
    val output = all - num
    val output2 = sc.parallelize(Seq(output)).repartition(1)
    output2.saveAsTextFile(args(1))
  }
}
