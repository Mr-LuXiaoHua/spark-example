package com.example

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{SaveMode, SparkSession}



/**
 * MongoDB读写操作
 */
object MongoTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .master("local[*]") // 本地开发模式
                            .appName("Spark-MonoDB-Test")

                            // 读取配置
                            // MongoDB设置有密码的话，需要通过MongoDB的admin数据库进行认证
                            // 读取配置
                            // MongoDB设置有密码的话，需要通过MongoDB的admin数据库进行认证
                            .config("spark.mongodb.input.uri", "mongodb://root:123456@192.168.0.192:27017/admin")
                            // 要读取的数据库
                            .config("spark.mongodb.input.database", "test")
                            // 要读取的集合
                            .config("spark.mongodb.input.collection", "users")


                            // 写入配置
                            // MongoDB设置有密码的话，需要通过MongoDB的admin数据库进行认证
                            .config("spark.mongodb.output.uri", "mongodb://root:123456@192.168.0.192:27017/admin")
                            // 要写入的数据库
                            .config("spark.mongodb.output.database", "test")
                            // 要读取的集合

                            .getOrCreate()


    val sc = spark.sparkContext
    val usersDF = MongoSpark.load(sc).toDF()

    usersDF.createOrReplaceTempView("v_temp_users")

    val users = spark.sql("select name, age from v_temp_users")
    users.show()

    MongoSpark.write(users).option("collection", "users3").mode(SaveMode.Overwrite).save()

  }
}
