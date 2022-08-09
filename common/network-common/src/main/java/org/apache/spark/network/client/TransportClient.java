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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.*;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private final TransportResponseHandler handler;
  @Nullable private String clientId;
  private volatile boolean timedOut;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  /**
   * Returns the ID used by the client to authenticate itself when authentication is enabled.
   *
   * @return The client ID, or null if authentication is disabled.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the authenticated client ID. This is meant to be used by the authentication layer.
   *
   * Trying to set a different client ID after it's been set will result in an exception.
   */
  public void setClientId(String id) {
    Preconditions.checkState(clientId == null, "Client ID has already been set.");
    this.clientId = id;
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   *
   * 从远端协商好的流中请求单个块
   * fetchChunk(...)方法除了存放回调对象的方式和构造的消息对象类型与sendRpc(...)方法不同以外，功能实现上几乎是一致的。
   * 对于ChunkReceivedCallback类型的回调对象，存放位置是TransportResponseHandler的outstandingFetches字典，键为StreamChunkId对象；
   * 另外，fetchChunk(...)方法最终构造的消息对象类型是ChunkFetchRequest。
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,
      int chunkIndex,
      ChunkReceivedCallback callback) {
    if (logger.isDebugEnabled()) {
      logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
    }

    StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    StdChannelListener listener = new StdChannelListener(streamChunkId) {
      @Override
      void handleFailure(String errorMsg, Throwable cause) {
        handler.removeFetchRequest(streamChunkId);
        callback.onFailure(chunkIndex, new IOException(errorMsg, cause));
      }
    };
    handler.addFetchRequest(streamChunkId, callback);

    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(listener);
  }

  /**
   * Request to stream the data with the given stream ID from the remote end.
   * 使用流ID，从远端获取数据
   * stream(...)方法中存放回调的结构是队列（TransportResponseHandler的streamCallbacks队列），
   * 这与前面的两个方法是不同的。客户端在获取流的时候，只能够按照顺序获取，且服务端每次只会处理一个流请求，
   * 因此请求和响应都是通过队列来管理的，以FIFO模式的保证顺序。
   * 另外，stream(...)方法最终构造的消息对象类型是StreamRequest。
   *
   * @param streamId The stream to fetch.
   * @param callback Object to call with the stream data.
   */
  public void stream(String streamId, StreamCallback callback) {
    StdChannelListener listener = new StdChannelListener(streamId) {
      @Override
      void handleFailure(String errorMsg, Throwable cause) throws Exception {
        callback.onFailure(streamId, new IOException(errorMsg, cause));
      }
    };
    if (logger.isDebugEnabled()) {
      logger.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
    }

    // Need to synchronize here so that the callback is added to the queue and the RPC is
    // written to the socket atomically, so that callbacks are called in the right order
    // when responses arrive.
    synchronized (this) {
      handler.addStreamCallback(streamId, callback);
      channel.writeAndFlush(new StreamRequest(streamId)).addListener(listener);
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   *
   * sendRpc(...)方法的第一个参数是要发送的消息数据，第二个参数则是包装了收到响应后需要进行的操作的回调对象。
   * sendRpc(...)方法会为每个RpcRequest设定一个Request ID，然后将Request ID和回调对象进行关联，
   * 存放到TransportResponseHandler的outstandingRpcs字典中，而服务端在对应的响应消息中也会携带相同的Request ID，
   * 这样一来，客户端在收到响应之后，可以根据Request ID查找当时存放在TransportResponseHandler的outstandingRpcs字典中的回调对象，
   * 使用该回调对象进行响应处理。这种设计方法在分布式框架中是非常常见的。
   *
   * @param message The message to send.
   * @param callback Callback to handle the RPC's reply.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    // 使用UUID生产请求主键
    long requestId = requestId();
    /*
     * 注意这里的操作，向TransportResponseHandler添加requestId和RpcResponseCallback的引用关系。
     * 该TransportResponseHandler是在向Bootstrap的处理器链中添加TransportChannelHandler时添加的，
     * 这一步操作会将requestId和回调进行关联，在客户端收到服务端的响应消息时，响应消息中是携带了相同的requestId的，
     * 此时就可以通过requestId从TransportResponseHandler中获取当时发送请求时设置的回调，
     * 达到通过响应结果处理回调的效果。
     */
    handler.addRpcRequest(requestId, callback);

    /*
     * 在使用Channel写出数据后，会添加一个监听器监听写出情况，如果写出失败，
     * 会从TransportResponseHandler的outstandingRpcs字典中移除当时存放的回调对象，
     * 直接调用回调对象的onFailure(...)方法以告知失败情况。
     */
    RpcChannelListener listener = new RpcChannelListener(requestId, callback);
    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
      .addListener(listener);

    return requestId;
  }

  /**
   * Send data to the remote end as a stream.  This differs from stream() in that this is a request
   * to *send* data to the remote end, not to receive it from the remote.
   *
   * @param meta meta data associated with the stream, which will be read completely on the
   *             receiving end before the stream itself.
   * @param data this will be streamed to the remote end to allow for transferring large amounts
   *             of data without reading into memory.
   * @param callback handles the reply -- onSuccess will only be called when both message and data
   *                 are received successfully.
   */
  public long uploadStream(
      ManagedBuffer meta,
      ManagedBuffer data,
      RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    long requestId = requestId();
    handler.addRpcRequest(requestId, callback);

    RpcChannelListener listener = new RpcChannelListener(requestId, callback);
    channel.writeAndFlush(new UploadStream(requestId, meta, data)).addListener(listener);

    return requestId;
  }

  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
      // 通过SettableFuture实现同步发送消息
    final SettableFuture<ByteBuffer> result = SettableFuture.create();

    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        ByteBuffer copy = ByteBuffer.allocate(response.remaining());
        copy.put(response);
        // flip "copy" to make it readable
        copy.flip();
        result.set(copy);
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });

    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   *
   * @param message The message to send.
   */
  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  /**
   * Removes any state associated with the given RPC.
   *
   * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
   */
  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  /** Mark this channel as having timed out. */
  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress())
      .add("clientId", clientId)
      .add("isActive", isActive())
      .toString();
  }

  private static long requestId() {
    return Math.abs(UUID.randomUUID().getLeastSignificantBits());
  }

  private class StdChannelListener
      implements GenericFutureListener<Future<? super Void>> {
    final long startTime;
    final Object requestId;

    StdChannelListener(Object requestId) {
      this.startTime = System.currentTimeMillis();
      this.requestId = requestId;
    }

    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      if (future.isSuccess()) {
        if (logger.isTraceEnabled()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          logger.trace("Sending request {} to {} took {} ms", requestId,
              getRemoteAddress(channel), timeTaken);
        }
      } else {
        String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
            getRemoteAddress(channel), future.cause());
        logger.error(errorMsg, future.cause());
        channel.close();
        try {
          handleFailure(errorMsg, future.cause());
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    }

    void handleFailure(String errorMsg, Throwable cause) throws Exception {}
  }

  private class RpcChannelListener extends StdChannelListener {
    final long rpcRequestId;
    final RpcResponseCallback callback;

    RpcChannelListener(long rpcRequestId, RpcResponseCallback callback) {
      super("RPC " + rpcRequestId);
      this.rpcRequestId = rpcRequestId;
      this.callback = callback;
    }

    @Override
    void handleFailure(String errorMsg, Throwable cause) {
      handler.removeRpcRequest(rpcRequestId);
      callback.onFailure(new IOException(errorMsg, cause));
    }
  }

}
