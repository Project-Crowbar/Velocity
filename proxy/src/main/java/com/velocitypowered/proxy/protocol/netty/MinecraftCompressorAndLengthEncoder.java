/*
 * Copyright (C) 2018-2023 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.protocol.netty;

import static com.velocitypowered.proxy.protocol.netty.MinecraftVarintLengthEncoder.IS_JAVA_CIPHER;

import com.velocitypowered.natives.compression.VelocityCompressor;
import com.velocitypowered.natives.util.MoreByteBufUtils;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.util.VelocityProperties;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.util.zip.DataFormatException;

/**
 * Handler for compressing Minecraft packets.
 */
public class MinecraftCompressorAndLengthEncoder extends MessageToByteEncoder<ByteBuf> {

  private static final int VANILLA_MAXIMUM_PACKET_SIZE = 1 << 21; // 2MiB
  private static final int HARD_MAXIMUM_PACKET_SIZE = Integer.MAX_VALUE;

  private static final int CLIENTBOUND_PACKET_CAP =
      VelocityProperties.readBoolean("velocity.increased-compression-cap", true)
          ? HARD_MAXIMUM_PACKET_SIZE : VANILLA_MAXIMUM_PACKET_SIZE;

  private int threshold;
  private final VelocityCompressor compressor;

  public MinecraftCompressorAndLengthEncoder(int threshold, VelocityCompressor compressor) {
    this.threshold = threshold;
    this.compressor = compressor;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
    int uncompressed = msg.readableBytes();
    if (uncompressed < threshold) {
      // Under the threshold, there is nothing to do.
      ProtocolUtils.writeVarInt(out, uncompressed + 1);
      out.writeByte(0);
      out.writeBytes(msg);
    } else {
      handleCompressed(ctx, msg, out);
    }
  }

  private void handleCompressed(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out)
      throws DataFormatException {
    int uncompressed = msg.readableBytes();

    // Write compressed payload to a temporary buffer
    int estimatedSize = uncompressed + ProtocolUtils.varIntBytes(uncompressed);
    ByteBuf payload = MoreByteBufUtils.preferredBuffer(ctx.alloc(), compressor, estimatedSize);
    try {
      ProtocolUtils.writeVarInt(payload, uncompressed);
      ByteBuf compatibleIn = MoreByteBufUtils.ensureCompatible(ctx.alloc(), compressor, msg);
      try {
        compressor.deflate(compatibleIn, payload);
      } finally {
        compatibleIn.release();
      }

      int payloadLength = payload.readableBytes();
      if (payloadLength >= CLIENTBOUND_PACKET_CAP) {
        throw new DataFormatException("The server sent a very large (over "
            + CLIENTBOUND_PACKET_CAP + " byte) compressed packet.");
      }

      ProtocolUtils.writeVarInt(out, payloadLength);
      out.writeBytes(payload);
    } finally {
      payload.release();
    }
  }

  @Override
  protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, ByteBuf msg, boolean preferDirect) {
    int uncompressed = msg.readableBytes();
    if (uncompressed < threshold) {
      int finalBufferSize = uncompressed + 1;
      finalBufferSize += ProtocolUtils.varIntBytes(finalBufferSize);
      return IS_JAVA_CIPHER
          ? ctx.alloc().heapBuffer(finalBufferSize)
          : ctx.alloc().directBuffer(finalBufferSize);
    }

    // (maximum data length after compression) + max outer varint + uncompressed data varint
    int initialBufferSize = (uncompressed - 1) + 5 + ProtocolUtils.varIntBytes(uncompressed);
    return MoreByteBufUtils.preferredBuffer(ctx.alloc(), compressor, initialBufferSize);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    compressor.close();
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }
}
