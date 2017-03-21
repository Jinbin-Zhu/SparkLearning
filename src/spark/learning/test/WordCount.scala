package spark.learning.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Jinbin Zhu on 3/21/17.
  */
class WordCount {


}

object WordCount {

  def main(args: Array[String]): Unit = {

    if(args.length < 1){
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val line = sc.textFile(args(0))

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect.foreach(println)

    sc.stop()
  }
}
