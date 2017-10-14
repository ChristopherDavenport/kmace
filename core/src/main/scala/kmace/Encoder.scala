package kmace


trait Encoder[A] {
  def toBytes(a: A): Array[Byte]
}

object Encoder {

  def apply[A](f: A => Array[Byte]) =  new Encoder[A]{
    def toBytes(a: A): Array[Byte] = f(a)
  }

  implicit def optionEncoder[A](implicit encoder: Encoder[A]) = new Encoder[Option[A]]{
    override def toBytes(a: Option[A]): Array[Byte] = a.fold[Array[Byte]](null)(encoder.toBytes)
  }

}


