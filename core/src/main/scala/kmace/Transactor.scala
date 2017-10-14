package kmace

import scalaz._
import Scalaz._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{Cell, CellUtil, TableName}
import scalaz.stream._
import scalaz.concurrent.Task


case class Transactor[F[_], A](a: A)(implicit C: Config[A]){

  def withConnection[B](f: Connection => Process[Task, B]) : Process[Task, B] =
    Process.bracket(
      Task.delay(ConnectionFactory.createConnection(C.toConfiguration(a)))
    )(s =>
      Process.eval(Task.delay(s.close())).drain
    )(f)


  def table(tableName: String): Process[Task, Table] =
    withConnection(conn => Process.eval(Task.delay(conn.getTable(TableName.valueOf(tableName)))))

  def get[K, V](tableName: String, k: K)
               (implicit E: Encoder[K], D: Decoder[K], S: Decoder[String]): Process[Task, Option[(K, List[Option[Field[Array[Byte]]]])]] = {
    table(tableName)
      .evalMap[Task, Result](t =>
        Task.delay(t.get(new Get(E.toBytes(k))))
      )
      .map(x => transformResult[K](x)(D, S))
  }

  /**
    * Retrieve all rows from table
    *
    */
  def scanAll[K](tableName: String)(implicit E: Encoder[K], D: Decoder[K], S: Decoder[String]): Process[Task, Map[K, List[Option[Field[Array[Byte]]]]]] = {

    table(tableName)
      .flatMap(t =>
        Process.bracket(
          Task.delay(t.getScanner(new Scan()))
        )(s =>
          Process.eval(Task.delay(s.close())).drain
        )(Process.emit)
      )
      .map(s => transformResults[K](s))
  }


  def transformResult[K](result: Result)
                        (implicit D: Decoder[K], S: Decoder[String]): Option[(K, List[Option[Field[Array[Byte]]]])] = {
    import scala.collection.JavaConverters._
    result.isEmpty
        .fold(
          D.fromBytes(result.getRow).map(n =>
            n -> result.listCells().asScala.map(cellToField).toList
          ),
          None
        )
  }

  def transformResults[K](scan: ResultScanner)
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

