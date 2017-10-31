package network

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.CharsetUtil
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import partner.FollowerState
import partner.JobRequest
import partner.JobType
import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.LongAdder

/**
 * Created by owen on 17/10/11.
 */
/**
 * 作为客户端，向Follower发送请求
 */
class LeaderCommunicationHandler(val input: ByteBuf) : SimpleChannelInboundHandler<ByteBuf>() {
    @Throws(Exception::class)
    override fun channelActive(ctx: ChannelHandlerContext) {
        ctx.writeAndFlush(input)
        //ctx.writeAndFlush(JobRequest(JobType.INSERT,"adasda"))
    }

    @Throws(Exception::class)
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }

    override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
        println(msg.toString(CharsetUtil.UTF_8))
    }
}

class LeaderCommunicationClient(private val input: ByteBuf,
                                private val addr: InetSocketAddress,
                                private val eventLoop: EventLoop,
                                private val jobAssigneds: LongAdder) {
    @Throws(InterruptedException::class)
    fun start() {


            val b = Bootstrap()
            b.group(eventLoop)
                    .channel(NioSocketChannel::class.java)
                    .remoteAddress(addr)
                    .handler(object : ChannelInitializer<SocketChannel>() {
                        @Throws(Exception::class)
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline().addLast(LeaderCommunicationHandler(input))
                        }
                    })

            val f = b.connect().sync()
            f.channel().closeFuture().sync()

    }



}





/**
 * 作为服务端，接收客户端的请求
 * TODO("这里需要使用事务的形式来玩")
 */
class LeaderServerHandler(val follower: ConcurrentHashMap<InetSocketAddress, String>) : EchoServerHandler(){


    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        //TODO("这里tnnd，任务的调度很难写呀")
        val input = msg as ByteBuf
        val sql = input.toString(CharsetUtil.UTF_8)
        val request = JobRequest.toJobRequest(sql)
        if (request.type == JobType.SELECT){
            //TODO("SELECT JOB")
        }else{
            try {
                MySQLUtil.executeSQL(request)
            }catch (e: Exception){
                ctx.writeAndFlush(Unpooled.copiedBuffer("failed",CharsetUtil.UTF_8))
            }

            //使用协程来玩

            var jobAssigned = LongAdder()
            val eventloop: EventLoop = ctx.channel().eventLoop()

            for ((k,v) in follower){
                if (v == FollowerState.FULL){
                    launch (CommonPool) {
                        LeaderCommunicationClient(input, k,
                                eventloop,
                                jobAssigned).start()
                    }
                }
            }

            ctx.writeAndFlush("success")

            println("$jobAssigned follower has received the job")
        }



    }


}

class LeaderServer(val port: Int, val follower: ConcurrentHashMap<InetSocketAddress, String>) {

    fun start() {
        val serverHandler = LeaderServerHandler(follower)
        //创建事件循环组
        val group = NioEventLoopGroup()
        val group2 = NioEventLoopGroup()
        try {
            //服务器启动器
            val bootstrap = ServerBootstrap()
            bootstrap.group(group, group2)
                    .channel(NioServerSocketChannel::class.java)
                    .localAddress(InetSocketAddress(port))
                    .childHandler(object : ChannelInitializer<SocketChannel>() {
                        @Throws(Exception::class)
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline().addLast(serverHandler)
                        }
                    })
            val f = bootstrap.bind().sync()
            f.channel().closeFuture().sync()
        } finally {
            group.shutdownGracefully().sync()
        }
    }

}