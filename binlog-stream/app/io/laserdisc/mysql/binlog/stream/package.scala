package io.laserdisc.mysql.binlog

import cats.data.State
import cats.effect.kernel.Ref
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.github.shyiko.mysql.binlog.event.Event
import org.typelevel.log4cats.Logger
import io.laserdisc.mysql.binlog.event.EventMessage

package object stream {
  def streamEvents[F[_]: Concurrent: Sync: Logger](
      transactionState: Ref[F, TransactionState],
      schema: String = null
  ): fs2.Pipe[F, Event, EventMessage] =
    _.through(streamTransactionPackages[F](transactionState, schema)).flatMap(pkg =>
      fs2.Stream.eval(warnBigTransactionPackage(pkg)) >> fs2.Stream(pkg.events: _*)
    )

  def streamCompactedEvents[F[_]: Concurrent: Logger](
      transactionState: Ref[F, TransactionState],
      schema: String
  ): fs2.Pipe[F, Event, EventMessage] =
    _.through(streamTransactionPackages[F](transactionState, schema)).flatMap(pkg => fs2.Stream(compaction.compact(pkg.events): _*))

  def streamTransactionPackages[F[_]: Concurrent: Logger](
      transactionState: Ref[F, TransactionState],
      schema: String
  ): fs2.Pipe[F, Event, TransactionPackage] =
    _.evalMap(event =>
      Logger[F].debug(s"received binlog event $event") >> transactionState.modifyState(
        try {
          TransactionState.nextState(event, schema)
        }catch {
          case e: Throwable =>
            Logger[F].debug(s"@debug received binlog event error $event")
            e.printStackTrace()
            State[TransactionState, Option[TransactionPackage]] { implicit transactionState: TransactionState =>
              (transactionState, None)
            }
        }
      )
    ).unNone

  def warnBigTransactionPackage[F[_]: Sync: Logger](
      transactionPackage: TransactionPackage
  ): F[Unit] =
    if (transactionPackage.events.size >= 1000)
      for {
        distro <- Sync[F].delay(transactionPackage.events.groupBy(_.table).map { case (k, v) =>
          k -> v.size
        })
        _ <- Logger[F].warn(s"""Transaction has > then 1000 elements in it with
                               |following distribution $distro
        """.stripMargin)
      } yield ()
    else
      Sync[F].unit

}
