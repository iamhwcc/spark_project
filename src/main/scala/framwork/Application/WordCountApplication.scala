package framwork.Application

import framwork.common.TApplication
import framwork.controller.WordCountController

object WordCountApplication extends App with TApplication {
    //启动应用程序
    start() {
        val controller = new WordCountController()
        controller.control()
    }
}
