package com.example;


import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * MongoDB读写操作
 */
public class MongoTestForJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Spark-MonoDB-Test")

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

                .getOrCreate();


        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> userDS = MongoSpark.load(jsc).toDF();

        userDS.printSchema();

        userDS.createOrReplaceTempView("v_temp_users");
        Dataset<Row> resultDS = spark.sql("SELECT name, age FROM v_temp_users");
        resultDS.show();

        // 将数据写入集合users2
        MongoSpark.write(resultDS).option("collection", "users2").mode(SaveMode.Overwrite).save();

        jsc.close();

    }
}
