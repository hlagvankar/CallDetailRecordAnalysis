import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Hdfs

object CallDetailRecord {
  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Please provide input and output directory path")
      sys.exit()
    } else {

      case class CDR(visitor_locn: String, call_duration: Integer, phone_no: String, error_code: String)
      val input = args(0)
      val output = args(1)
      val conf = new SparkConf().setAppName("Call Details Record")
      val sc = new SparkContext(conf)
      
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)

      try { hdfs.delete(new org.apache.hadoop.fs.Path(output), true) } catch { case _: Throwable => {} }

      val inputFile = sc.textFile(input, 2).cache()
      val data = inputFile.map(_.split(",")).map(p => CDR(p(0), p(1).toInt, p(2), p(3)))

      val dropCalls = data.map(x => (x.visitor_locn, 1)).reduceByKey(_ + _).collect.sortBy(_._2).reverse.take(10)
      sc.makeRDD(dropCalls).saveAsTextFile(output)
    }

  }
}
