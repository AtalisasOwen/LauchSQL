import org.junit.Test
import org.junit.Assert.*
import java.net.InetSocketAddress

/**
 * Created by owen on 17/10/10.
 */


class ByteUtilTest{

    @Test
    fun testParseAddr(){
        val s = "/follower/localhost:28938"
        val addr = ByteUtil.string2InetAddress(s)
        val followers = linkedMapOf<InetSocketAddress,String>()
        assertEquals(addr, InetSocketAddress("localhost",28938))
        followers.put(addr,"FULL")
        println(followers)
    }

}