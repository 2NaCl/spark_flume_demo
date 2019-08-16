package com.fyj.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

object spark_flume_push {

  def main(args: Array[String]): Unit = {

//    if(args.length!=2) {
//      System.exit(1)
//    }
//
//    var Array(hostname,port) = args

    var sc = new SparkConf().setAppName("spark-flume").setMaster("local[*]")
    var ssc = new StreamingContext(sc, Seconds(5))

    val flumeStream = FlumeUtils.createStream(ssc,"0.0.0.0",41414)
    flumeStream.map(x=> new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }

}
