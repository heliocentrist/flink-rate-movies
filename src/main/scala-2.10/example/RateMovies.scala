package org.example

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import net.liftweb.json._
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class MovieRating(movieId: Int, rating: Double)
case class RunningAverage (movieId: Int, rating: Double, number: Int)

object RateMovies {

  implicit val formats = DefaultFormats

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.getConfig.setGlobalJobParameters(params)

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream("localhost", 7999)

    val counts = text.map { t => parse(t).extract[MovieRating] }
      .keyBy("movieId")
      .fold(new RunningAverage(0, 0, 0))((acc, cur) => { new RunningAverage(cur.movieId, (cur.rating + acc.rating*acc.number) / (acc.number + 1), acc.number + 1) })
      .keyBy("movieId")
      .timeWindow(Time.seconds(1), Time.seconds(1))
      .apply { (
          cell: Tuple,
          window: TimeWindow,
          events: Iterable[RunningAverage],
          out: Collector[RunningAverage]) =>
        out.collect(events.last)
      }

    counts.writeAsText("/Users/yury.liavitski/personalwork/scala/moviesFlink/output.txt")

    env.execute("Scala SocketTextStreamWordCount Example")
  }
}
