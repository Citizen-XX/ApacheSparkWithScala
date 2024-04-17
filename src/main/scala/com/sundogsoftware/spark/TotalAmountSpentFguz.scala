package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object TotalAmountSpentFguz {

  def cleaning(line: String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amountSpent = fields(2).toFloat
    (customerId,amountSpent)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local","TotalAmountSpentFguz")

    val input = sc.textFile("data/customer-orders.csv")

    val records = input.map(cleaning)

    val sumSpent = records.reduceByKey((x,y) => x+y)

    val flippedSum = sumSpent.map(x => (x._2,x._1))

    val results = flippedSum.collect()

    for (result <- results.sorted) {
      val user = result._2
      val dollars = result._1
      println(f"User number $user spent a total of $dollars%.2f dollars")
    }

  }

}
