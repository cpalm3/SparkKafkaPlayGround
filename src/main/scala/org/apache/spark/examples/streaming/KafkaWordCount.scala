/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args
    var totalCount = 0L
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    // val input =  ssc.sparkContext.parallelize(List("pandas", "i like pandas"))
   // input.saveAsTextFile("/tmp/streamPanda")

//messages.print()
   val lines = messages.map(_._2)  //Key is null, retrieve value
    lines.print()// only showing some of the lines, sourced data from eclipse with 1k, this wrote 1k , check flume setup

  

   lines.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
       totalCount = totalCount+ rdd.count()
        System.out.println("TotalCount "+totalCount+" Not Empty "+rdd.toString())
        rdd.saveAsTextFile("/tmp/lines"+System.currentTimeMillis())


      }
    })
    //val words = lines.flatMap(_.split(" "))
   // val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
   // wordCounts.print()

    /*  messages.count().foreachRDD(rdd=>{
      if(!rdd.partitions.isEmpty){

        rdd.saveAsTextFile("/tmp/wordCounts")

      }
    }) */

  //  messages.saveAsTextFiles("/tmp/wordCounts")

   // messages.saveAsHadoopFiles("/tmp/wordCounts","countFile")

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
