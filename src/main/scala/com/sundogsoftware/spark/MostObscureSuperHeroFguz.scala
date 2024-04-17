package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import scala.math.Ordering.Implicits.infixOrderingOps

object MostObscureSuperHeroFguz {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MostObscureSuperHero")
      .master("local[*]")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val leastConnections = connections.select(min(col("connections"))).first()(0)

    val mostObscure = connections.filter(col("connections") === leastConnections)
      .sort($"connections".desc)

    //val mostObscureWithNames = mostObscure.join(names, mostObscure("id") === names("id"),"left").select("name","connections")
    val mostObscureWithNames = mostObscure.join(names, usingColumn = "id")

    mostObscureWithNames.show()

    spark.close()
  }
}
