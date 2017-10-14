package kmace

trait Decoder[A] {
  def fromBytes(a: Array[Byte]): Option[A]
}

object Decoder {
  def apply[A](f: Array[Byte] => Option[A]): Decoder[A] = new Decoder[A] {
    def fromBytes(a: Array[Byte]): Option[A] = f(a)
  }
}
