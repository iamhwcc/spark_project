package framwork.common

import framwork.util.EnvUtil

trait TDao {
    def readData(path: String) = {
        //使用ThreadLocal中的环境
        EnvUtil.getEnv().textFile(path)
    }
}
