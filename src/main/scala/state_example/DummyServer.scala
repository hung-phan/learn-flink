package state_example

import java.io.{BufferedReader, FileReader, PrintWriter}
import java.net.ServerSocket

object DummyServer extends App {
  val listener = new ServerSocket(9090)

  var br: BufferedReader = null

  try {
    val socket = listener.accept()

    println(s"Got new connection: $socket")

    br = new BufferedReader(
      new FileReader(getClass.getResource("/state_example/broadcast_small").getPath)
    )

    try {
      val out = new PrintWriter(socket.getOutputStream, true)
      var line: String = null

      while ((line = br.readLine()) != null) {
        out.println(line)
      }
    } finally {
      socket.close()
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    listener.close()

    if (br != null) {
      br.close()
    }
  }
}
