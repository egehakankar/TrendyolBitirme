package com.trendyol.bootcamp.batch

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import collection.mutable._
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession, DataFrame, Row, SQLContext, DataFrameReader}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, IntegerType, TimestampType}

import scala.util.Try

object SparkAllJobs {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkJobs")
      .getOrCreate()

    // If you want to see all logs, set log level to info
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    /*val ordersSchema = Encoders.product[Order].schema
    val orders = spark.read
      .schema(ordersSchema)
      .json("raw-data/orders")
     .as[Order]*/

     val ordersSchema2 = Encoders.product[Order2].schema
    val orders2 = spark.read
      .schema(ordersSchema2)
      .json("raw-data/orders")
     .as[Order2]

     /*val productsSchema = Encoders.product[Product].schema
    val products = spark.read
      .schema(productsSchema)
      .json("raw-data/products")
     .as[Product]*/

    //-----Job3------

    val j3 = orders2
    .groupBy("product_id", "order_date", "price")
    .agg(count("product_id"))
    .orderBy($"product_id", asc("order_date"))
    .toDF

    var rowBefore = ""
    var priceBefore: Double = 1.0
    var las3 = Seq(("", 0, "")).toDF("product_id", "price", "change")

    j3.collect().foreach(row => 
      {
        if(row(0) != null && row(1) != null && row(2) != null )
        {
          val rowP = row(2).toString.toDouble
          if(rowBefore == row(0).toString)
          {
            if(rowP < priceBefore)
            {
              priceBefore = rowP
              las3 = las3.union(Seq((row(0).toString, rowP, "fall")).toDF("product_id", "price", "change"))
            }
            else if(rowP > priceBefore)
            {
              priceBefore = rowP
              las3 = las3.union( Seq((row(0).toString, rowP, "fall")).toDF("product_id", "price", "change"))
            }
          }
          else
          {
            priceBefore = rowP
            rowBefore = row(0).toString
          }
        }
      }
    )

    las3.show()

    //-----Job 2---
/*    val jon = orders2
    .join(products, orders2("product_id") === products("productid"), "inner")
    .drop(orders2("product_id"))

    val cate = jon
    .filter($"status" === "Created")
    .groupBy(dayofmonth($"order_date") as "day", $"seller_id" as "seller", $"categoryname" as "category")
    .count()

    val window2 = Window.partitionBy("day", "seller").orderBy(desc("category"))

    val topCat = cate
    .withColumn("order", row_number().over(window2))
    .filter($"order" === 1)
    .drop("count", "order")
    .orderBy("day", "seller")

    val best10 = jon
    .filter($"status" === "Created")
    .groupBy(dayofmonth($"order_date") as "day", $"seller_id")
    .agg(sum("price") as "revenue", count("productid") as "count")
    .orderBy(asc("day"), desc("revenue"))
    .as[TopT]

    val window = Window.partitionBy("day").orderBy(desc("revenue"))

    val bestt = best10
    .withColumn("rank", rank().over(window))
    .filter($"rank" < 11)
    .orderBy(asc("day"), asc("rank"))

    val joinJ2 = topCat
    .join(bestt, topCat("seller") === bestt("seller_id") && topCat("day") === bestt("day"), "inner")
    .drop(topCat("seller"))
    .drop( topCat("day"))
    .select($"day", $"seller_id", $"category", $"count")
    .orderBy("day")
    .toDF

    joinJ2
      .repartition(1)
      .write
      .partitionBy("day")
      .mode(SaveMode.Overwrite)
      .json("output/batch/job2")



    //----Job 1----
    val brut = orders
    .groupBy("product_id")
    .agg(avg("price") as "net_price", count("*") as "net_amount")
    
    
    val net = orders
    .filter(($"status" !== "Returned") and ($"status" !== "Cancelled"))
    .groupBy("product_id")
    .agg(avg("price") as "gross_price", count("*") as "gross_amount")

    val joinedS = brut
    .join(net, brut("product_id") === net("product_id"), "inner")
    .drop(brut("product_id"))
    .select($"product_id", $"net_price", $"net_amount", $"gross_price", $"gross_amount")

    val locc = orders
    .groupBy("product_id","location").count()
    .groupBy("product_id")
    .max("count")

    val locc2 = orders
    .groupBy("product_id","location").count()

    val locc3 = locc
    .join(locc2, locc("max(count)") === locc2("count") && locc("product_id") === locc2("product_id"), "inner")
    .drop("max(count)","count")
    .drop(locc("product_id"))

    val lastt = orders
    .filter($"order_date" > "2021-01-22")
    .groupBy("product_id")
    .agg((count("*") / 5) as "average_sales_last_5")

     val joinedS2 = locc3
    .join(lastt, locc3("product_id") === lastt("product_id"), "inner")
    .drop(locc3("product_id"))
    .select($"product_id", $"average_sales_last_5", $"location" as "top_location")

    val resultD = joinedS
    .join(joinedS2, joinedS("product_id") === joinedS2("product_id"), "inner")
    .drop(joinedS("product_id"))
    .select($"product_id", $"net_price", $"net_amount", $"gross_price", $"gross_amount", $"average_sales_last_5", $"top_location")
    .dropDuplicates("product_id")
    .toDF

      resultD
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json("output/batch/job1")*/

    spark.stop()
  }

}

case class TopT(day: Int, seller_id: String, revenue: Double, count: BigInt)
case class Order2(customer_id: String, location: String, order_date: java.sql.Timestamp,  order_id: String, price: Double, product_id: String, seller_id: String, status: String)
case class Product(brandname: String, categoryname: String, productid: String, productname: String)
case class Order(customer_id: String, location: String, order_date: String,  order_id: String, price: Double, product_id: String, seller_id: String, status: String)