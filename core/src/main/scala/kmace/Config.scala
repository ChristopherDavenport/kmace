package kmace

import org.apache.hadoop.conf.Configuration

trait Config[A] {
  def toConfiguration(a: A) : Configuration
}

object Config {

  def apply[A](f: A => Configuration) = new Config[A] {
    def toConfiguration(a: A): Configuration = f(a)
  }

}
