/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.util.LinkedList;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A customized frame decoder that allows intercepting raw data.
 * <p>
 * This behaves like Netty's frame decoder (with hard coded parameters that match this library's
 * needs), except it allows an interceptor to be installed to read data directly before it's
 * framed.
 * <p>
 * Unlike Netty's frame decoder, each frame is dispatched to child handlers as soon as it's
 * decoded, instead of building as many frames as the current buffer allows and dispatching
 * all of them. This allows a child handler to install an interceptor if needed.
 * <p>
 * If an interceptor is installed, framing stops, and data is instead fed directly to the
 * interceptor. When the interceptor indicates that it doesn't need to read any more data,
 * framing resumes. Interceptors should not hold references to the data buffers provided
 * to their handle() method.
 *
 * 对从管道中读取的ByteBuf按照数据帧进行解析。
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {

  // 处理器名
  public static final String HANDLER_NAME = "frameDecoder";
  // 表示帧大小的数据长度
  private static final int LENGTH_SIZE = 8;
  // 最大帧大小，为Integer.MAX_VALUE
  private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
  // 标记字段，用于标记帧大小记录是无效的
  private static final int UNKNOWN_FRAME_SIZE = -1;

  /**
   * 存储ByteBuf的链表，收到的ByteBuf会存入该链表
    */
  private final LinkedList<ByteBuf> buffers = new LinkedList<>();
  // 存放帧大小的ByteBuf
  private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE, LENGTH_SIZE);

  // 当次总共可读字节
  private long totalSize = 0;
  // 下一个帧的大小
  private long nextFrameSize = UNKNOWN_FRAME_SIZE;
  // 拦截器
  private volatile Interceptor interceptor;

  /**
   * 当收到一个ByteBuf，channelRead(...)方法会先将其存入到buffers链表，并将其可读字节数添加到totalSize，
   * 然后在buffers链表不为空的情况下，不断取出链表头的ByteBuf进行处理，其实这里buffers链表充当了一个FIFO的队列，保证了ByteBuf的顺序。
   *
   * 取到的ByteBuf会根据是否有拦截器分别进行处理；
   *    拦截器会对消息进行拦截，在Spark中实现的可用拦截器只有StreamInterceptor，是用于流传输的场景；
   *    在有拦截器的情况下，仅仅是将在拦截操作中读取的数据丢弃并维护可读字节数记录，并没有做其他的操作；
   *    另外需要注意的是，拦截器会在检查通过后被移除，否则会被理由为“Interceptor still active but buffer has data.”的断言终止。
   *
   * 在没有拦截器的情况下，会调用decodeNext()方法进行解码帧数据，该方法才是拆包的关键
   *
   * @param ctx
   * @param data
   * @throws Exception
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
    // 将传入的数据转换为Netty的ByteBuf
    ByteBuf in = (ByteBuf) data;
    // 添加到LinkedList类型的buffers链表中进行记录
    buffers.add(in);
    // 增加总共可读取的字节数
    totalSize += in.readableBytes();

    // 遍历buffers链表
    while (!buffers.isEmpty()) {
      // First, feed the interceptor, and if it's still, active, try again.
      // 有拦截器，让拦截器处理，拦截器只会处理一次
      if (interceptor != null) {
        // 取出链表头的ByteBuf
        ByteBuf first = buffers.getFirst();
        // 计算可读字节数
        int available = first.readableBytes();
        // 先使用intercepter处理数据
        if (feedInterceptor(first)) {
          assert !first.isReadable() : "Interceptor still active but buffer has data.";
        }

        // 计算已读字节数
        int read = available - first.readableBytes();
        // 如果全部读完，就将该ByteBuf从buffers链表中移除
        if (read == available) {
          buffers.removeFirst().release();
        }
        // 维护可读字节计数
        totalSize -= read;
      } else {
        // Interceptor is not active, so try to decode one frame.
        // 没有拦截器，尝试解码帧数据
        ByteBuf frame = decodeNext();
        // 解码出来的帧数据为null，直接跳出循环
        if (frame == null) {
          break;
        }
        // 能够解码得到数据，传递给下一个Handler
        ctx.fireChannelRead(frame);
      }
    }
  }

  private long decodeFrameSize() {
    if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
      return nextFrameSize;
    }

    // We know there's enough data. If the first buffer contains all the data, great. Otherwise,
    // hold the bytes for the frame length in a composite buffer until we have enough data to read
    // the frame size. Normally, it should be rare to need more than one buffer to read the frame
    // size.
    ByteBuf first = buffers.getFirst();
    if (first.readableBytes() >= LENGTH_SIZE) {
      nextFrameSize = first.readLong() - LENGTH_SIZE;
      totalSize -= LENGTH_SIZE;
      if (!first.isReadable()) {
        buffers.removeFirst().release();
      }
      return nextFrameSize;
    }

    while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
      ByteBuf next = buffers.getFirst();
      int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
      frameLenBuf.writeBytes(next, toRead);
      if (!next.isReadable()) {
        buffers.removeFirst().release();
      }
    }

    nextFrameSize = frameLenBuf.readLong() - LENGTH_SIZE;
    totalSize -= LENGTH_SIZE;
    frameLenBuf.clear();
    return nextFrameSize;
  }

  private ByteBuf decodeNext() {
    // 得到帧大小
    long frameSize = decodeFrameSize();
    // 检查帧大小的合法性
    if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
      return null;
    }

    // Reset size for next frame.
    nextFrameSize = UNKNOWN_FRAME_SIZE;

    Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE, "Too large frame: %s", frameSize);
    Preconditions.checkArgument(frameSize > 0, "Frame length should be positive: %s", frameSize);

    // If the first buffer holds the entire frame, return it.
    // 剩余可读帧数
    int remaining = (int) frameSize;
    /*
     * 如果buffers中第一个ByteBuf的可读字节数大于等于可读帧数，
     * 表示这一个ByteBuf包含了一整个帧的数据，可以一次读到一个帧
     */
    if (buffers.getFirst().readableBytes() >= remaining) {
      return nextBufferForFrame(remaining);
    }

    // Otherwise, create a composite buffer.
    // 此时说明一个ByteBuf的数据不够一个帧，构造一个复合ByteBuf
    CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
    // 当还没读到一个帧的数据时，循环处理
    while (remaining > 0) {
      /*
       * 获取buffers链表头的ByteBuf中remaining长度的数据
       * 读取过程中如果将链表头的ByteBuf读完了，会将其从buffers中移除
       */
      ByteBuf next = nextBufferForFrame(remaining);
      // 减去读到的数据
      remaining -= next.readableBytes();
      // 添加到CompositeByteBuf
      frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
    }
    assert remaining == 0;
    // 终于读完一个帧了，返回复合ByteBuf
    return frame;
  }

  /**
   * Takes the first buffer in the internal list, and either adjust it to fit in the frame
   * (by taking a slice out of it) or remove it from the internal list.
   */
  private ByteBuf nextBufferForFrame(int bytesToRead) {
    ByteBuf buf = buffers.getFirst();
    ByteBuf frame;

    if (buf.readableBytes() > bytesToRead) {
      frame = buf.retain().readSlice(bytesToRead);
      totalSize -= bytesToRead;
    } else {
      frame = buf;
      buffers.removeFirst();
      totalSize -= frame.readableBytes();
    }

    return frame;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    for (ByteBuf b : buffers) {
      b.release();
    }
    if (interceptor != null) {
      interceptor.channelInactive();
    }
    frameLenBuf.release();
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (interceptor != null) {
      interceptor.exceptionCaught(cause);
    }
    super.exceptionCaught(ctx, cause);
  }

  public void setInterceptor(Interceptor interceptor) {
    Preconditions.checkState(this.interceptor == null, "Already have an interceptor.");
    this.interceptor = interceptor;
  }

  /**
   * @return Whether the interceptor is still active after processing the data.
   */
  private boolean feedInterceptor(ByteBuf buf) throws Exception {
    if (interceptor != null && !interceptor.handle(buf)) {
      interceptor = null;
    }
    return interceptor != null;
  }

  public interface Interceptor {

    /**
     * Handles data received from the remote end.
     *
     * @param data Buffer containing data.
     * @return "true" if the interceptor expects more data, "false" to uninstall the interceptor.
     */
    boolean handle(ByteBuf data) throws Exception;

    /** Called if an exception is thrown in the channel pipeline. */
    void exceptionCaught(Throwable cause) throws Exception;

    /** Called if the channel is closed and the interceptor is still installed. */
    void channelInactive() throws Exception;

  }

}
