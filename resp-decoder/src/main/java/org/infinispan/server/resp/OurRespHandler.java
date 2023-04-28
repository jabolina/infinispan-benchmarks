package org.infinispan.server.resp;

import static org.infinispan.server.resp.ByteBufferUtils.bytesToResult;
import static org.infinispan.server.resp.ByteBufferUtils.stringToByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.commands.string.GET;
import org.infinispan.server.resp.commands.string.SET;

import io.netty.channel.ChannelHandlerContext;

public class OurRespHandler extends RespRequestHandler {
   private static final CompletableFuture<byte[]> GET_FUTURE = CompletableFuture.completedFuture(new byte[] { 0x1, 0x12});
   public static byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
      if (type instanceof GET)
         return stageToReturn(GET_FUTURE, ctx, GET_TRICONSUMER);

      if (type instanceof SET)
         return stageToReturn(CompletableFutures.completedNull(), ctx, OK_BICONSUMER);

      return super.handleRequest(ctx, type, arguments);
   }

   protected static final BiConsumer<Object, ByteBufPool> OK_BICONSUMER = (ignore, alloc) ->
         alloc.acquire(OK.length).writeBytes(OK);

   protected static final BiConsumer<byte[], ByteBufPool> GET_TRICONSUMER = (innerValueBytes, alloc) -> {
      if (innerValueBytes != null) {
         bytesToResult(innerValueBytes, alloc);
      } else {
         stringToByteBuf("$-1\r\n", alloc);
      }
   };
}
