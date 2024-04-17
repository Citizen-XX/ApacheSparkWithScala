package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object TotalSpentByCustomerFguz {

  case class Customer(cust_id: Int, amount_spent: Double, item_id:Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("TotalSpentByCustomerFguz").master("local[*]").getOrCreate()

    val customerSchema = new StructType().add("cust_id", IntegerType, nullable = true).add("amount_spent",DoubleType, nullable = true).add("item_id", IntegerType, nullable = true)

    import spark.implicits._
    val customer = spark.read.schema(customerSchema).csv("data/customer-orders.csv").as[Customer]

    println("This is the inferred schema: ")
    customer.printSchema()

    val customerOrders = customer.select("cust_id","amount_spent")

    customerOrders.show(5)

    val custOrdersGroupByID = customerOrders.groupBy("cust_id").sum("amount_spent")

    custOrdersGroupByID.show(10)

    val custOrdersGroupIDSort = custOrdersGroupByID.withColumn("total_spent", round($"sum(amount_spent)",2)).select("cust_id","total_spent").sort("total_spent")

    custOrdersGroupIDSort.show(custOrdersGroupIDSort.count.toInt)

    spark.close()

  }

}
