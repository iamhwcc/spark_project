package distributedTest

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    //建立客户端，与服务器连接
    val client = new Socket("localhost",9999);

    val outputStream = client.getOutputStream
    val objOutput = new ObjectOutputStream(outputStream)//输出一个对象
    val task = new Task()
    objOutput.writeObject(task)
    objOutput.flush()
    objOutput.close()
    client.close()
    println("客户端发送数据完毕!")
  }

}
