package com.example

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .appName("WordCount")
                            .master("local[*]")
                            .getOrCreate()

    import spark.implicits._

    val wordsDS:Dataset[String] = spark.read.textFile("words.txt")
    val wordCount = wordsDS.flatMap(_.split(" ")).groupBy("value").count()
    wordCount.show()

    spark.close()
  }
}
