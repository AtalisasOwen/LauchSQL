import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress

/**
 * Created by owen on 17/10/9.
 */
class ByteUtil{

    companion object{

        fun byte2InetAddress(b: ByteArray): InetSocketAddress{
            val s = String(b)
            val address: List<String> = s.split(":")
            val host: String = address[0]
            val port: Int = Integer.parseInt(address[1])
            return InetSocketAddress(host,port)
        }

        fun string2InetAddress(s: String): InetSocketAddress{
            val str = s.substring(10)
            val address: List<String> = str.split(":")
            val host: String = address[0]
            val port: Int = Integer.parseInt(address[1])
            //println("$host:$port")
            return InetSocketAddress(host,port)
        }


    }

}
