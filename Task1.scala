import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => line.split(","))
    val maxvalue = output.map{
         _.drop(1)
         .filter(_.trim != "")
         .map(_.toInt)
         .max
         }
    val value2 = (output zip maxvalue).map {
        case((x),(y))=>
         x.zipWithIndex.filter(pair => pair._1.equals(y.toString)).map(pair => pair._2)
         }

    val value3 = (output zip value2).map{
        case((x),(y))=>
        x(0)+","+(y.mkString(","))
        }

    value3.saveAsTextFile(args(1))
  }
}
