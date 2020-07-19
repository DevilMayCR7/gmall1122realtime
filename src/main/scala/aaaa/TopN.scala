package aaaa

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("topN").setMaster("local[*]")
    val ssc = new SparkContext(conf)
    val rdd: RDD[String] = ssc.textFile("O:\\Java\\gmall1122realtime\\src\\main\\resources\\1.txt")
    rdd.mapPartitions(partition=>{
      partition.map(item=>{
        (item, 1)
      })
    }).reduceByKey(_+_).sortBy(_._2).take(3)

    ssc.stop()
  }
}
