package io.laserdisc.mysql.binlog.models

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.ForAllTestContainer
import db.MySqlContainerTest
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SchemaMetadataTest extends AnyWordSpec with ForAllTestContainer with MySqlContainerTest with Matchers {

  "Schema Metadata" should {

    "restore schema state from DB" in {

      implicit val testTransactor: Aux[IO, Unit] =
        Transactor.fromDriverManager[IO](
          mySQLContainer.driverClassName,
          s"${mySQLContainer.jdbcUrl}?useSSL=false",
          mysqlDBCreds,
          None
        )
      val schemaState =
        SchemaMetadata.buildSchemaMetadata("test").unsafeRunSync()

      schemaState.tables should have size 2
      schemaState.tables("sku").columns should have size 2
      schemaState
        .tables("sku")
        .columns
        .values
        .filter(_.isPk)
        .head
        .name should be("id")
      schemaState.tables("variant").columns should have size 2
      schemaState
        .tables("variant")
        .columns
        .values
        .filter(_.isPk)
        .head
        .name should be("id")
    }
  }

}
