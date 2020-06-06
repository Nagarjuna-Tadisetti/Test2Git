package com.visitors.enrich.spark

import org.apache.spark.sql.SparkSession
import org.json.{JSONArray, JSONObject}

import scala.collection.immutable.Map

object SparkVisitorEnrich {

  def visitorEnrich(visitor:String, broadcastedMap: org.apache.spark.broadcast.Broadcast[Map[String, String]]): String = {
      var visitorJsonObject = new JSONObject(visitor)
      val productsArray = visitorJsonObject.getJSONArray("products")
      val joinedProductsArray = enrichProductName(productsArray,broadcastedMap)
      visitorJsonObject.put("products",joinedProductsArray)
      visitorJsonObject.toString(4)
  }

  def enrichProductName(products:JSONArray,broadcastedMap: org.apache.spark.broadcast.Broadcast[Map[String, String]]):JSONArray = {
    var joinedProductsArray = new JSONArray()
    for(j <- 0 until products.length()) {
      var product = products.getJSONObject(j)
      val productName = broadcastedMap.value.get(product.getString("id"))
      if(productName.isDefined) {
        product = product.put("name", productName.head)
      }
      else {
        product = product.put("name", "unknown")
      }
      joinedProductsArray.put(product)
    }
    joinedProductsArray
  }



  def main(args: Array[String]): Unit = {
    val rec1: String =
      """{
    "visitorId": "v1",
    "products": [{
         "id": "i1",
         "interest": 0.68
    }, {
         "id": "i2",
         "interest": 0.42
    }]
    }"""

    val rec2: String = """{
    "visitorId": "v2",
    "products": [{
         "id": "i1",
         "interest": 0.78
    }, {
         "id": "i3",
         "interest": 0.11
    }]
    }"""

    val visitsData: Seq[String] = Seq(rec1, rec2)

    val productIdToNameMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella", "i3" -> "Jeans")

    val spark = SparkSession.builder()
                .appName("Visitor Enrich")
                .master("local")
                .getOrCreate()

    val broadcastedMap = spark.sparkContext.broadcast(productIdToNameMap)

    val visitsRDD = spark.sparkContext.parallelize(visitsData)

    val enrichedRDD = visitsRDD.map(visitRecord => visitorEnrich(visitRecord,broadcastedMap))
    println(enrichedRDD.collect().mkString(","))
  }

}
