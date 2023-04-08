package io.laserdisc.mysql.binlog.stream

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.concurrent.TrieMap

class TrieMapSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  var trieMap: TrieMap[String, Int] = _

  override def beforeEach(): Unit =
    trieMap = TrieMap[String, Int]("a" -> 1, "b" -> 2)

  override def afterEach(): Unit =
    trieMap.clear()

  "A TrieMap" should {

    "insert elements" in {
      trieMap += ("c" -> 3)
      trieMap.size should be(3)
      trieMap("c") should be(3)
    }

    "query elements" in {
      trieMap.get("a") should be(Some(1))
      trieMap.get("c") should be(None)
      trieMap("b") should be(2)
    }

    "remove elements" in {
      trieMap -= "a"
      trieMap.size should be(1)
      trieMap.get("a") should be(None)
      trieMap("b") should be(2)
    }
  }

  "TrieMap size" should {
    val trieMapMemory: TrieMap[String, Int] = TrieMap.empty
    val size = 100_000
    "100_000 size" in {
      println("start TrieMap size: " + trieMapMemory.size)
      for (i <- 1 to size)
        trieMapMemory += (i.toString -> i)
      println("end TrieMap size: " + trieMapMemory.size)
      trieMapMemory.size should be(size)
    }
  }

}
