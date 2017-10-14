package kmace.instances

import kmace.Decoder
import org.apache.hadoop.hbase.util.Bytes

import scala.util.Try

trait DecoderInstances {
  implicit val stringDecoder = genDecoder(Bytes.toString)
  implicit val intDecoder = Decoder[Int](i => Try(Bytes.toInt(i)).toOption)
  implicit val longDecoder = Decoder[Long](l => Try(Bytes.toLong(l)).toOption)
  implicit val booleanDecoder = Decoder[Boolean](b => Try(Bytes.toBoolean(b)).toOption)
  implicit val doubleDecoder = Decoder[Double](d => Try(Bytes.toDouble(d)).toOption)
  implicit val floatDecoder = Decoder[Float](f => Try(Bytes.toFloat(f)).toOption)
  implicit val bigDecimalDecoder = Decoder[BigDecimal](bd => Try(BigDecimal(Bytes.toBigDecimal(bd))).toOption)
  implicit val shortDecoder = Decoder[Short](short => Try(Bytes.toShort(short)).toOption)
  implicit val arrayByteDecoder = Decoder[Array[Byte]](a => Option(a))

  // As it is truly unlikely any Array[Byte] can go to an A. But that's the signature
  // We are given from Hadoop
  private def genDecoder[A](f: Array[Byte] => A): Decoder[A] =
    Decoder[A](a => Option(a).flatMap(a => Try(f(a)).toOption))

}
