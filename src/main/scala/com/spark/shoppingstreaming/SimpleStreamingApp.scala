package com.spark.shoppingstreaming

import java.io.PrintWriter
import java.net.ServerSocket
import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


/**
 * A simple Spark Streaming app in Scala
 */
object SimpleStreamingApp {

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    // here we simply print out the first few elements of each batch
          // split and print (user, product, price)
    // Split each line into words
    val words = stream.flatMap(_.split(","))
    stream.print()
    ssc.start()
    ssc.awaitTermination()

  }
}