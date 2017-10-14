package kmace

import cats.implicits._
import cats.effect.Effect
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{Cell, CellUtil, TableName}
import fs2._


case class Transactor[F[_], A](a: A)(implicit F: Effect[F], C: Config[A]){

  def withConnection[B](f: Connection => Stream[F, B]) : Stream[F, B] = Stream.bracket(
    F.delay(ConnectionFactory.createConnection(C.toConfiguration(a)))
  )(f,
    s => F.delay(s.close())
  )

  def table(tableName: String): Stream[F, Table] =
    withConnection(conn => Stream.eval(F.delay(conn.getTable(TableName.valueOf(tableName)))))


  def get[K, V](tableName: String, k: K)
               (implicit E: Encoder[K], D: Decoder[K], S: Decoder[String]): Stream[F, Option[(K, List[Option[Field[Array[Byte]]]])]] = {
    table(tableName).evalMap(t =>
      F.delay(t.get(new Get(E.toBytes(k))))).map(x => transformResult[K](x)(D, S))
  }

  def transformResult[K](result: Result)
                        (implicit D: Decoder[K], S: Decoder[String]): Option[(K, List[Option[Field[Array[Byte]]]])] = {
    import scala.collection.JavaConverters._
    if (result.isEmpty) None
    else D.fromBytes(result.getRow).map(n =>
      n -> result.listCells().asScala.map(cellToField).toList
    )
  }

  private[this] def transformResults[K](scan: ResultScanner)
                                       (implicit D: Decoder[K], S: Decoder[String]): Map[K, List[Option[Field[Array[Byte]]]]] = {
    import scala.collection.JavaConverters._
    scan.asScala.flatMap{ x =>transformResult[K](x) }.toMap
  }

  def cellToField(cell: Cell)(implicit SD: Decoder[String]): Option[Field[Array[Byte]]] = {
    for {
      family <- SD.fromBytes(CellUtil.cloneFamily(cell))
      qualifier <- SD.fromBytes(CellUtil.cloneQualifier(cell))
      value <- CellUtil.cloneValue(cell).some
    } yield Field[Array[Byte]](family, qualifier, value)

  }


}

