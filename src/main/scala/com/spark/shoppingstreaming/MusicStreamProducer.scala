package com.spark.shoppingstreaming

import java.io.PrintWriter
import java.net.ServerSocket
import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random



object MusicStreamProducer {
    def main(args: Array[String]) {

    // Maximum number of events per second
    val MaxEvents = 5

  val bufferedSource = scala.io.Source.fromFile("Top_Trending_top20_40_price_discount.csv" ,"iso-8859-1")
  //println(bufferedSource.getLines.drop(1))
  
  var music_array : List[((((String, String), String), String),String)] = List()
  for (line <- bufferedSource.getLines.drop(1)) {
    val cols = line.split(",").map(_.trim)
    // do whatever you want with the columns here
    val music_info = Seq( {cols(0)} -> {cols(2)} -> {cols(3)} -> {cols(4)} -> {cols(5)} )
    music_array = music_array ++ music_info
    //println(music_info)
    //println(s"${cols(0)}|${cols(2)}|${cols(3)}|${cols(4)}|${cols(5)}")
  }
  //println(music_array)
  bufferedSource.close
  
  val random = new Random()
  val names = Seq("Miguel Eric","James Juan","Shawn")
  
    /** Generate a number of random sales events */
  def generateSaleEvents(n: Int) = {
      (1 to n).map { i =>
        val ((((rank,artist), music),price),discount) = music_array(random.nextInt(music_array.size))
        val user = random.shuffle(names).head
        //suppose formula (copies-discount) 0.9 max 2 copies, 0.3 max 8 copies
        val maxcopy = (11-10*discount.toFloat)
        val copies = random.nextInt(maxcopy.toInt)+2
        (user, rank, artist, music, price, discount, copies)
      }
    }
  println(generateSaleEvents(20))

    // create a network producer
    val listener = new ServerSocket(9999)
    println("Listening on port: 9999")

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)+1
            val productEvents = generateSaleEvents(num)
            productEvents.foreach{ event =>
              out.write(event.productIterator.mkString(","))
              out.write("\n")
            }
            out.flush()
            println(s"Created $num events...")
          }
          socket.close()
        }
      }.start()
    }
  }
}