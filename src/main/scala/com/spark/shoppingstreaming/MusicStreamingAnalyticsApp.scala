package com.spark.shoppingstreaming

import java.io.PrintWriter
import java.net.ServerSocket
import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * A more complex Streaming app, which computes statistics and prints the results for each batch in a DStream
 */
object MusicStreamingAnalyticsApp {

    def main(args: Array[String]) {

    val ssc = new StreamingContext("local[2]", "Michael Music Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    // create stream of events from raw text elements
    val events = stream.map { record =>
      val event = record.split(",")
      (event(0), event(1), event(2), event(3), event(4), event(5), event(6))
    }

    /*
      We compute and print out stats for each batch.
      Since each batch is an RDD, we call forEeachRDD on the DStream, and apply the usual RDD functions
      we used in Chapter 1.
     */
    events.foreachRDD { (rdd, time) =>
      val numPurchases = rdd.count()
      val uniqueUsers = rdd.map { case (user, _, _, _, _, _, _) => user }.distinct().count()
      val uniqueArtist = rdd.map { case (_,  _, artist, _, _, _, _) => artist }.distinct().count()
      val totalRevenue = rdd.map { case (_, _, _, _, price, discount, copies) => price.toDouble*discount.toDouble*copies.toInt }.sum()
            
      /*val artistByRevenue = rdd
        .map { case (user, artist, music, price, discount, copies) => (artist, price.toDouble*discount.toDouble*copies.toInt) }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)      
      val mostRevenueArtist = artistByRevenue(0) */   
      
      val musicByPopularity = rdd
        .map { case (user, rank, artist, music, price, discount, copies) => (music, copies.toInt) }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)      
      val mostMusicPopular = musicByPopularity(0)
      
      val artistByPopularity = rdd
        .map { case (user, rank, artist, music, price, discount, copies) => ("Rank:"+rank+"  artist:"+artist, copies.toInt) }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)    
      var quantity = artistByPopularity.length
      val mostArtistPopular = artistByPopularity(0)
      

      val formatter = new SimpleDateFormat
      val dateStr = formatter.format(new Date(time.milliseconds))
      println(s"== Batch start time: $dateStr ==")
      //println("Unique users: " + uniqueUsers)
      //println("Unique artists: " + uniqueArtist)
      //println("Total revenue: " + totalRevenue)
      //println("Most Revenue Artist: %s with %d Revenue".format(mostRevenueArtist._1, mostRevenueArtist._2))     
      //println("Most popular music: %s with %d purchases".format(mostMusicPopular._1, mostMusicPopular._2))
      //quantity-1
      println("\n\n\n"+" ---------------Output top artist sales quantity result----------------")
      for( a <- 0 to quantity-1 ){
         println("Popular: %s with %d purchases".format(artistByPopularity(a)._1, artistByPopularity(a)._2));
      }
      println("\n\n\n"+" ---------------End Output----------------")
      //println("Most popular artist: %s with %d purchases".format(mostArtistPopular._1, mostArtistPopular._2))
    }

    // start the context
    ssc.start()
    ssc.awaitTermination()

  }
}