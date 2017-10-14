package kmace.instances

import kmace.Decoder
import org.apache.hadoop.hbase.util.Bytes

import scala.util.Try

trait DecoderInstances {
  implicit val stringDecoder = genDecoder(Bytes.toString)
  implicit val intDecoder = genDecoder(Bytes.toInt)
  implicit val longDecoder = genDecoder(Bytes.toLong)
  implicit val booleanDecoder = genDecoder(Bytes.toBoolean)
  implicit val doubleDecoder = genDecoder(Bytes.toDouble)
  implicit val floatDecoder = genDecoder(Bytes.toFloat)
  implicit val shortDecoder = genDecoder(Bytes.toShort)
  implicit val bigDecimalDecoder = Decoder[BigDecimal](bd =>
    Option(bd).flatMap(bd => Try(BigDecimal(Bytes.toBigDecimal(bd))).toOption)
  )
  implicit val arrayByteDecoder = Decoder[Array[Byte]](a => Option(a))

  // As it is truly unlikely any Array[Byte] can go to an A. But that's the signature
  // We are given from Hadoop
  private def genDecoder[A](f: Array[Byte] => A): Decoder[A] =
    Decoder[A](a => Option(a).flatMap(a => Try(f(a)).toOption))

}
