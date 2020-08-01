package window_example

import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

object DummyServer extends App {
  val listener = new ServerSocket(9090)

  try {
    val socket = listener.accept()

    println(s"Got new connection: $socket")

    try {
      val out = new PrintWriter(socket.getOutputStream, true)
      val rand = new Random()

      while (true) {
        val int = rand.nextInt(100)

        out.println(s"${System.currentTimeMillis()},$int")
        Thread.sleep(50)
      }
    } finally {
      socket.close()
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    listener.close()
  }
}
