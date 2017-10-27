import org.junit.Test
import partner.Follower
import java.net.InetSocketAddress

/**
 * Created by owen on 17/10/11.
 */

class FollowerNetworkTest {

    fun startFollowerServer(){
        val jedis = ClientUtil.createJedis()
        val curator = ClientUtil.createCurator()
        val leader = InetSocketAddress(1111)
        val follower = Follower(leader, jedis, curator)
        follower.fakeInit()
    }

}

fun main(args: Array<String>){
    FollowerNetworkTest().startFollowerServer()


    Thread.sleep(100000)
}