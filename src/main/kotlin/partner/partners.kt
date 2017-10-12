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

class Leader(val jedis: Jedis, val curator: CuratorFramework) : Partner{

    val lock = CountDownLatch(1)

    val follower = ConcurrentHashMap<InetSocketAddress,String>()


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

    }
}


class Follower(var leader: InetSocketAddress?,
               private val jedis: Jedis?,
               private val curator: CuratorFramework?) : Partner{
    private val lock = CountDownLatch(1)
    private val key = ClientUtil.getServerPort()

    private val tasks = LinkedBlockingQueue<JobRequest>()

    @Volatile
    var working = true

    //启动两个线程
    private val receivedThread = ReceiveJobThread()
    private val executedThread = ExecuteJobThread()


    private inner class ReceiveJobThread : Thread("Follower-Receiver"){
        override fun run() {
            val port = ClientUtil.getPort()
            FollowerServer(port,tasks).start()
            println("ReceiveJobThread Quit")
        }
    }

    private inner class ExecuteJobThread : Thread("Follower-Executor"){
        override fun run() {
            while (working){
                val job = tasks.take()
                //TODO:这里是测试，记得改掉
                MySQLUtil.executeSQL(job)
            }
            println("ExecuteJobThread Quit")
        }
    }


    //TODO:这个方法为了测试
    @TestOnly
    fun fakeInit(): Boolean{
        val r = ReceiveJobThread()
        val e = ExecuteJobThread()

        r.isDaemon = false
        e.isDaemon = false

        r.start()
        e.start()

        return true
    }

    override fun init(): Boolean{
        watchLeaderReady()
        lock.await()
        curator?.create()
                ?.creatingParentsIfNeeded()
                ?.withMode(CreateMode.EPHEMERAL)
                ?.forPath("/follower/${ClientUtil.getServerPort()}",
                        FollowerState.FULL.toByteArray())
        jedis?.lpop(key)

        receivedThread.isDaemon = false
        executedThread.isDaemon = false

        receivedThread.start()
        executedThread.start()

        return true
    }

    private fun watchLeaderReady(){
        val cache = NodeCache(curator,"/ready")
        cache.listenable.addListener(NodeCacheListener {
            val data: String = String(cache.currentData.data)
            setLeaderAddr(data)
            lock.countDown()
        })
        cache.start()
    }

    private fun setLeaderAddr(data: String){
        val aAndP = data.split(":")
        if (leader == null){
            leader = InetSocketAddress(aAndP[0],Integer.parseInt(aAndP[1]))
        }
    }


    override fun close() {
        curator?.close()
        jedis?.close()
        working = false
        receivedThread.interrupt()
    }
}

class Looker(val jedis: Jedis,
             val curator: CuratorFramework) : Partner{

    var leaderAddr: InetSocketAddress? = null

    override fun init(): Boolean {
        val key = ClientUtil.getServerPort()
        jedis.lpush(key,"")
        jedis.expire(key,10)
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
