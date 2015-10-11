import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD,PairRDDFunctions}

import java.util.StringTokenizer
import scala.collection.mutable.ListBuffer

object WordCount{
  val delimiters : String = " \t,;.?!-:@[](){}_*/";

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile = sc textFile args(0)

    val rdd = textFile.flatMap(line => {
          val st = new StringTokenizer(line,delimiters)
          val ls = ListBuffer[String]()
          while(st.hasMoreTokens()){
            ls += st.nextToken().trim().toLowerCase(); 
          }
          ls.toList
        })
      .filter(_.size > 0)
      .map(w => (w, 1))
      .reduceByKey(_+_)
      .sortBy(- _._2) // reverse sort

    rdd saveAsTextFile args(1)
    sc.stop()
  }
}
