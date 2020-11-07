package real_usecase

import java.io.{BufferedReader, FileReader, PrintWriter}
import java.net.ServerSocket

object BankDataServer extends App {
  val listener = new ServerSocket(9090)

  var br: BufferedReader = null

  try {
    val socket = listener.accept

    println("Got new connection: " + socket.toString)

    br = new BufferedReader(
      new FileReader(
        getClass.getResource("/real_usecase/bank_data.txt").getPath
      )
    )

    try {
      val out = new PrintWriter(socket.getOutputStream, true)
      var line: String = null

      while ((line = br.readLine()) != null) {
        out.println(line)

        Thread.sleep(500)
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
