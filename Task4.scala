import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val scoreLists = textFile
      .map(line => {
        val tmp = line.split(",", -1)
        val name = tmp(0)
        val scores = tmp.drop(1)
        (name, scores)
      })
      .collectAsMap()
    val scoreListsMap = sc.broadcast(scoreLists)

    val output = textFile
      .flatMap(line => {
        val tmp = line.split(",", -1)
        val name1 = tmp(0)
        val scores1 = tmp.drop(1)
        scoreListsMap.value.filterKeys(name2 => name2 > name1).flatMap(entry => {
          val name2 = entry._1
          val scores2 = entry._2
          val sim = scores1.zip(scores2).count(x => x._1 == x._2 && x._1 != "")
          List(name1 + "," + name2 + "," + sim) 
        })
      })
    sc.parallelize(Seq(output.collect().mkString("\n"))).coalesce(1).saveAsTextFile(args(1))
  }
}
