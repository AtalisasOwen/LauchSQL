package partner

import network.EchoServer
import network.FollowerServer
import network.LeaderServer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.*

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.jetbrains.annotations.TestOnly
import redis.clients.jedis.Client
import redis.clients.jedis.Jedis

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue

/**
 * Created by owen on 17/10/9.
 */

interface Partner{
    fun init(): Boolean
    fun close()
}


/**
 * 这是每台服务器的初始状态
 * 会竞争在Zookeeper中创建/leader
 * 如果创建成功，则会称为Leader,        => Leader.kt
 * 如果创建失败，则会获取Leader的IP地址  => Follower.kt
 *
 */
class Looker(val jedis: Jedis,
             val curator: CuratorFramework) : Partner{

    var leaderAddr: InetSocketAddress? = null

    override fun init(): Boolean {
        try {
            curator.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath("/leader",ClientUtil.getServerPort().toByteArray())
        }catch (e: KeeperException.NodeExistsException){
            val addr = curator.data
                    .forPath("/leader")
            leaderAddr = ByteUtil.byte2InetAddress(addr)
            return false
        }

        return true
    }

    fun turnToLeader(): Leader{
        println("Turn to Leader")
        return Leader(jedis, curator)
    }

    fun turnToFollower(): Follower{
        println("Turn to Follower")
        return Follower(leaderAddr,jedis,curator)
    }

    override fun close() {

    }
}
