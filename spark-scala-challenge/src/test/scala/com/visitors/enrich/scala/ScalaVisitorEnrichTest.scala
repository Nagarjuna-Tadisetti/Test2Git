package com.visitors.enrich.scala

import org.json.JSONObject
import org.junit.Assert._
import org.junit._

import scala.collection.immutable.Map

@Test
class ScalaVisitorEnrichTest {

  @Test
  def testMatchingProductId() = {
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

    val rec2: String =
      """{
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

    val actualEnrichedVisitors = ScalaVisitorEnrich.visitorEnrich(visitsData, productIdToNameMap)

    val v1Visitor = actualEnrichedVisitors.map(visitor => new JSONObject(visitor)).filter(jObject => jObject.getString("visitorId").equals("v1")).head

    val actualV1VisitorJoinedFirstProductName = v1Visitor.getJSONArray("products").getJSONObject(0).getString("name")
    val expectedV1VisitorJoinedFirstProductName = "Nike Shoes"
    assertTrue(actualV1VisitorJoinedFirstProductName == expectedV1VisitorJoinedFirstProductName)

    val actualV1VisitorJoinedSecondProductName = v1Visitor.getJSONArray("products").getJSONObject(1).getString("name")
    val expectedV1VisitorJoinedSecondProductName = "Umbrella"
    assertTrue(actualV1VisitorJoinedSecondProductName == expectedV1VisitorJoinedSecondProductName)

    val v2Visitor = actualEnrichedVisitors.map(visitor => new JSONObject(visitor)).filter(jObject => jObject.getString("visitorId").equals("v2")).head

    val actualV2VisitorJoinedFirstProductName = v2Visitor.getJSONArray("products").getJSONObject(0).getString("name")
    val expectedV2VisitorJoinedFirstProductName = "Nike Shoes"
    assertTrue(actualV2VisitorJoinedFirstProductName == expectedV2VisitorJoinedFirstProductName)

    val actualV2VisitorJoinedSecondProductName = v2Visitor.getJSONArray("products").getJSONObject(1).getString("name")
    val expectedV2VisitorJoinedSecondProductName = "Jeans"
    assertTrue(actualV2VisitorJoinedSecondProductName == expectedV2VisitorJoinedSecondProductName)
  }

  @Test
  def testNoMatchingProductId() = {
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

    val rec2: String =
      """{
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

    val productIdToNameMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella")

    val actualEnrichedVisitors = ScalaVisitorEnrich.visitorEnrich(visitsData, productIdToNameMap)

    val expectedString = """"name": "unknown""""
    assertTrue(actualEnrichedVisitors.mkString(",").contains(expectedString))
  }

}


