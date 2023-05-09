package io.laserdisc.mysql.binlog

import cats.effect.Sync
import cats.implicits._
import com.github.shyiko.mysql.binlog.BinaryLogClient
import org.typelevel.log4cats.Logger
import io.laserdisc.mysql.binlog.checkpoint.BinlogOffset
import io.laserdisc.mysql.binlog.config.BinLogConfig

package object client {

  def createBinLogClient[F[_]: Sync: Logger](
      config: BinLogConfig,
      offset: Option[BinlogOffset] = None,
      serverId: Long = 65535
  ): F[BinaryLogClient] =
    for {
      client <- Sync[F].delay(config.mkBinaryLogClient(offset, serverId))
      _ <-
        Logger[F].info(
          s"Binlog client ${config.schema} created with offset ${client.getBinlogFilename} ${client.getBinlogPosition}, serverId: $serverId"
        )
    } yield client
}
