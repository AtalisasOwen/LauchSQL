package network

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.util.CharsetUtil

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
