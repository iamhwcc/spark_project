package distributedTest

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {
  //Executor只负责计算，计算逻辑、数据等由Driver那边发过来
  def main(args: Array[String]): Unit = {
    //启动服务器，接受数据
    val serverSocket = new ServerSocket(9999)
    println("等待客户端连接......")

    //等待客户端连接
    val socket = serverSocket.accept()

    val inputStream = socket.getInputStream
    val objInput = new ObjectInputStream(inputStream)
    //readObject返回的是Any类型，asInstanceOf[Task]需要转成Task类才能使用里面的方法
    val task = objInput.readObject().asInstanceOf[Task]
    val ans = task.compute()
    println(ans)
    objInput.close()
    serverSocket.close()
  }

}
