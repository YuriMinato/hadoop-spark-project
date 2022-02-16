import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile
      .flatMap(line => {
        val tmp = line.split(",", -1).drop(1)
        var scores = (for {i <- tmp.indices} yield (i + 1, if (tmp(i) == "") 0 else 1)).toList
        scores
      }).reduceByKey(_ + _).map(x => x._1 + "," + x._2)
    
    output.saveAsTextFile(args(1))
  }
}
