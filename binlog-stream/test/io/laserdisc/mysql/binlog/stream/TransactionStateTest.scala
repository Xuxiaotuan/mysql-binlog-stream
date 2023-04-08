package io.laserdisc.mysql.binlog.stream

import _root_.io.circe.optics.JsonPath._
import _root_.io.laserdisc.mysql.binlog._
import com.github.shyiko.mysql.binlog.event._
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.{io, util}
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Queue

class TransactionStateTest extends AnyWordSpec with Matchers with OptionValues {

  def createRotateEvent: Event = {
    val header = new EventHeaderV4()
    header.setEventType(EventType.ROTATE)
    header.setTimestamp(System.currentTimeMillis())
    val data = new RotateEventData()
    data.setBinlogFilename("file.123")
    data.setBinlogPosition(4)
    new Event(header, data)
  }
  def createBeginEvent: Event = {
    val header = new EventHeaderV4()
    header.setEventType(EventType.QUERY)
    header.setTimestamp(System.currentTimeMillis())
    val data = new QueryEventData()
    data.setSql("BEGIN")
    new Event(header, data)
  }
  def createInsertEvent(): Event = {
    val header = new EventHeaderV4()
    header.setEventType(EventType.EXT_WRITE_ROWS)
    header.setTimestamp(System.currentTimeMillis())
    val data = new WriteRowsEventData()
    val rows: util.List[Array[io.Serializable]] =
      new util.ArrayList[Array[io.Serializable]]()
    rows.add(
      Array(543.asInstanceOf[io.Serializable], "sku3".getBytes.asInstanceOf[io.Serializable])
    )
    data.setRows(rows)
    val set = new util.BitSet()
    set.set(0, 2)
    data.setIncludedColumns(set)
    data.setTableId(123L)
    new Event(header, data)
  }

  def createTableMapEvent(): Event = {
    val header = new EventHeaderV4()
    header.setEventType(EventType.TABLE_MAP)
    header.setTimestamp(System.currentTimeMillis())
    val data = new TableMapEventData()
    data.setTableId(123)
    data.setTable("sku")
    data.setColumnTypes(Array(3.asInstanceOf[Byte], 15.asInstanceOf[Byte]))
    new Event(header, data)
  }

  def createXAEvent(): Event = {
    val header = new EventHeaderV4()
    header.setEventType(EventType.XID)
    header.setTimestamp(System.currentTimeMillis())
    val data = new XidEventData()
    data.setXid(122345)
    new Event(header, data)
  }

  def createPreviousGTIDsEvent(): Event = {
    val header = new EventHeaderV4()
    header.setEventType(EventType.PREVIOUS_GTIDS)
    header.setTimestamp(System.currentTimeMillis())
    val data = new XidEventData()
    data.setXid(122345)
    new Event(header, data)
  }

  "Transaction State" should {
    "start transaction on BEGIN event" in {
      TransactionState
        .nextState(createBeginEvent)
        .run(
          TransactionState(
            Queue.empty,
            schemaMetadata = models.SchemaMetadata.empty,
            fileName = "file.123",
            offset = 0,
            timestamp = 0
          )
        )
        .value match {
        case (state, None) =>
          state.transactionEvents should have size 0
          state.isTransaction should be(true)
        case res => fail(s"Assertion failed with $res")
      }
    }
    "accumulate events within transaction" in {
      val skuMeta =
        models.TableMetadata(
          "sku",
          Map(
            1 -> models.ColumnMetadata("id", "int", 1, isPk = true),
            2 -> models.ColumnMetadata("sku", "varchar", 2, isPk = false)
          )
        )
      val schemaMeta =
        models
          .SchemaMetadata(tables =  TrieMap("sku" -> skuMeta) , idToTable = TrieMap(123L -> "sku"))
      val res = for {
        _      <- TransactionState.nextState(createRotateEvent)
        _      <- TransactionState.nextState(createBeginEvent)
        _      <- TransactionState.nextState(createTableMapEvent())
        _      <- TransactionState.nextState(createInsertEvent())
        _      <- TransactionState.nextState(createTableMapEvent())
        _      <- TransactionState.nextState(createInsertEvent())
        _      <- TransactionState.nextState(createPreviousGTIDsEvent()) // should be ignored
        events <- TransactionState.nextState(createXAEvent())
      } yield events
      res
        .run(
          TransactionState(
            Queue.empty,
            schemaMetadata = schemaMeta,
            fileName = "file.1234",
            offset = 0,
            timestamp = 0
          )
        )
        .value match {
        case (state, txnPackage) =>
          state.isTransaction should be(false)
          state.transactionEvents should be(empty)
          txnPackage.value.events should have size 2
          txnPackage.value.events.forall(_.fileName == "file.123") should be(true)
          txnPackage.value.events.map(_.endOfTransaction) should be(List(false, true))
      }
    }

    "transform binlog write event into json" in {
      val skuMeta =
        models.TableMetadata(
          "sku",
          Map(
            1 -> models.ColumnMetadata("id", "int", 1, isPk = true),
            2 -> models.ColumnMetadata("sku", "varchar", 2, isPk = false)
          )
        )
      val schemaMeta =
        models
          .SchemaMetadata(tables = TrieMap("sku" -> skuMeta) , idToTable = TrieMap(123L -> "sku"))

      val json = TransactionState.convertToJson(
        tableMeta = schemaMeta.tables("sku"),
        includedColumns = Array(0, 1),
        timestamp = 12345L,
        action = "create",
        fileName = "file.12345",
        offset = 5363,
        record = (
          None,
          Some(
            Array(
              Some(1.asInstanceOf[io.Serializable]),
              Some("sku1".getBytes.asInstanceOf[io.Serializable])
            )
          )
        )
      )
      val _pk  = root.id.int
      val _id  = root.after.id.int
      val _sku = root.after.sku.string
      json.table should be("sku")
      json.timestamp should be(12345L)
      _id.getOption(json.row).value should be(1)
      _sku.getOption(json.row).value should be("sku1")
      _pk.getOption(json.pk).value should be(1)
    }

    "extract 'truncated table sku' from SQL" in {
      models.QueryEventData.truncateTable("truncate table sku").value should be("sku")
    }

    "extract 'truncated sku' from SQL" in {
      models.QueryEventData.truncateTable("truncate sku").value should be("sku")
    }
  }
}
