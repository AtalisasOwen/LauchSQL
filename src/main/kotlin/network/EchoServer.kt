package network

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.CharsetUtil

import java.net.InetSocketAddress

/**
 * Created by owen on 17/9/19.
 */


/**
 * Created by owen on 17/9/19.

 * 至少一个ChannelHandler
 * 配置服务器的启动代码
 */
//标识一个ChannelHandler可以被多个Channel安全的共享
@ChannelHandler.Sharable
open class EchoServerHandler : ChannelInboundHandlerAdapter() {


    //对每个传入的消息都要调用
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {

        println(msg::class.java)

        val input = msg as ByteBuf
        println("Server received: " + input.toString(CharsetUtil.UTF_8))

        val su = "Success".toByteArray()
        val b = Unpooled.copiedBuffer(su)

        ctx.write(b)  //将消息原样写给发送者

    }

    //最后一次对channelRead()的调用是当前批量处理调用
    override fun channelReadComplete(ctx: ChannelHandlerContext) {
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                .addListener(ChannelFutureListener.CLOSE)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }
}


class EchoServer(private val port: Int) {

    fun start() {
        val serverHandler = EchoServerHandler()
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

fun main(args: Array<String>){
    EchoServer(12321).start()
}
