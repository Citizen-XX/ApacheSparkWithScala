package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object FriendsByAgeDataSetFguz {
  case class Friend(age: Int,id: Int, friends: Int, name: String)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()

    import spark.implicits._
    val friends = spark.read.option("header","true").option("inferSchema","true").csv("data/fakefriends.csv").as[Friend]

    println("This is the inferred Schema")
    friends.printSchema()

    println("Lets select the columns needed")
    friends.select("id","name","age","friends").show()

    println("Avg number of friends by Age: ")
    friends.select(friends("age"),friends("friends")).groupBy("age").avg("friends").orderBy("age").show()

    println("Avg with custom settings: ")
    friends.select("age","friends").groupBy("age").agg(round(avg("friends"),2).alias("friends_avg")).sort("age").show()

    spark.stop()
  }
}
