package network

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.CharsetUtil

import java.net.InetSocketAddress
import java.util.*

/**
 * Created by owen on 17/9/19.
 * 测试客户端
 */

class EchoClientHandler(val sql: String) : SimpleChannelInboundHandler<ByteBuf>() {
    @Throws(Exception::class)
    override fun channelActive(ctx: ChannelHandlerContext) {
        ctx.writeAndFlush(Unpooled.copiedBuffer(sql, CharsetUtil.UTF_8))
        //ctx.writeAndFlush(JobRequest(JobType.INSERT,"adasda"))
    }

    @Throws(Exception::class)
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }

    override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {

        try {

            println(msg.toString(CharsetUtil.UTF_8))
        } catch (e: Exception) {
            println("asdasd")
        }

    }
}


class EchoClient(val sql: String) {
    private val host = "localhost"
    private val port = 12321

    @Throws(InterruptedException::class)
    fun start() {
        val group = NioEventLoopGroup()
        try {
            val b = Bootstrap()
            b.group(group)
                    .channel(NioSocketChannel::class.java)
                    .remoteAddress(InetSocketAddress(host, port))
                    .handler(object : ChannelInitializer<SocketChannel>() {
                        @Throws(Exception::class)
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline().addLast(EchoClientHandler(sql))
                        }
                    })

            val f = b.connect().sync()
            f.channel().closeFuture().sync()
        } finally {
            group.shutdownGracefully().sync()
        }
    }



}




fun main(args: Array<String>) {
    while (true){
        val input = Scanner(System.`in`)
        val sql = input.nextLine()
        if (sql == "exit") break
        EchoClient(sql).start()
    }

}

