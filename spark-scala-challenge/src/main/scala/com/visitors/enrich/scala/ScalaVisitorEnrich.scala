package com.visitors.enrich.scala

import org.json.{JSONArray, JSONObject}

import scala.collection.immutable.{List, Map}
import scala.collection.mutable

object ScalaVisitorEnrich {

  def visitorEnrich(visitors: Seq[String], productIdMap: Map[String, String]): List[String] = {

    var enrichedVisitors = new mutable.ListBuffer[String]
    visitors.foreach(visitor => {
      var visitorJsonObject = new JSONObject(visitor)
      val productsArray = visitorJsonObject.getJSONArray("products")
      val joinedProductsArray = enrichProductName(productsArray,productIdMap)
      visitorJsonObject.put("products",joinedProductsArray)
      enrichedVisitors += visitorJsonObject.toString(4)
    })
    enrichedVisitors.toList
  }

  def enrichProductName(products:JSONArray,productIdMap: Map[String, String]):JSONArray = {
    var joinedProductsArray = new JSONArray()
    for(j <- 0 until products.length()) {
      var product = products.getJSONObject(j)
      val productName = productIdMap.get(product.getString("id"))
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

    val enrichedVisitors = visitorEnrich(visitsData,productIdToNameMap)

    println(enrichedVisitors)
  }

}
