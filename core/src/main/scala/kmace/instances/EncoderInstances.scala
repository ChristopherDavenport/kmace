package kmace.instances

import kmace.Encoder
import org.apache.hadoop.hbase.util.Bytes

trait EncoderInstances {
  implicit val stringEncoder = Encoder[String]{s : String => Bytes.toBytes(s)}
  implicit val intEncoder = Encoder[Int]{i: Int => Bytes.toBytes(i)}
  implicit val longEncoder = Encoder[Long]{l: Long => Bytes.toBytes(l)}
  implicit val booleanEncoder = Encoder[Boolean]{b: Boolean => Bytes.toBytes(b)}
  implicit val doubleEncoder = Encoder[Double]{d: Double => Bytes.toBytes(d)}
  implicit val bigDecimalEncoder = Encoder[BigDecimal]{ bd: BigDecimal => Bytes.toBytes(bd.bigDecimal)}
  implicit val floatEncoder = Encoder[Float]{f: Float => Bytes.toBytes(f)}
  implicit val shortEncoder = Encoder[Short]{s: Short => Bytes.toBytes(s)}
  implicit val byteArayEncoder = Encoder[Array[Byte]]{a: Array[Byte] => a}
}
