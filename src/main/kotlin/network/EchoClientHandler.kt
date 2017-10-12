package network

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.util.CharsetUtil
import partner.JobRequest
import partner.JobType

import java.util.concurrent.ExecutorService

/**
 * Created by owen on 17/9/19.
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
