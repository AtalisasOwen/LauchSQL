package partner

import network.LeaderServer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.zookeeper.CreateMode
import redis.clients.jedis.Jedis
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch


/**
 * 成功创建/leader的节点
 * 在ReceiveJobThread中接收来自客户端的任务
 * 初始化时，Leader节点会放一个监听器，监听所有Follower的状态，并保存在ConcurrentHashMap中
 * 监听器创建完成后，会在Zookeeper中创建一个/ready
 * 之后子节点才能添加自己的IP地址
 *
 * 任务处理：
 * 1.select任务，直接分发给Follower
 * 2.其他任务，找到一半的可用子节点，将任务交给他们，保证一半以上的节点的一致性
 */
class Leader(private val jedis: Jedis,
             private val curator: CuratorFramework) : Partner{

    private val lock = CountDownLatch(1)

    private val follower = ConcurrentHashMap<InetSocketAddress,String>()


    private inner class ReceiveJobThread : Thread("Follower-Receiver"){
        override fun run() {
            val p = 5555
            LeaderServer(p,follower).start()
        }
    }


    override fun init(): Boolean {
        val cache = PathChildrenCache(curator,"/follower",true)
        cache.listenable.addListener(PathChildrenCacheListener{
            client, event ->
            val data = String(event.data.data)
            val path = ByteUtil.string2InetAddress(event.data.path)
            println("${event.type} -> $path:$data")
            when(event.type) {

                PathChildrenCacheEvent.Type.CHILD_ADDED -> {
                    follower.put(path, data)
                    println(follower)
                }

                PathChildrenCacheEvent.Type.CHILD_UPDATED ->
                    follower.put(path, data)

                PathChildrenCacheEvent.Type.CHILD_REMOVED ->
                    follower.remove(path, data)
            }
        })
        cache.start()
        setFollowerInit()

        lock.await()


        //TODO:之后要创建一个线程接受客户端任务,Netty
        ReceiveJobThread().start()
        //TODO:还要创建一个线程分发任务给Follower进行半二阶段提交,Netty

        return true
    }

    private fun setFollowerInit(){
        curator.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/ready",ClientUtil.getServerPort().toByteArray())
        lock.countDown()
    }

    fun assignRequest(request: Any){

    }

    override fun close() {
        val key = ClientUtil.getServerPort()
        jedis.del(key)
        curator.close()
        jedis.close()
    }
}
