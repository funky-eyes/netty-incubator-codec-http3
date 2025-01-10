/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.netty.incubator.codec.http3.example;

import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.*;
import io.netty.incubator.codec.http3.*;

import io.netty.incubator.codec.quic.QuicStreamChannel;

@ChannelHandler.Sharable
public class TripleHttp3PingPongHandler extends ChannelDuplexHandler {


    private final AtomicBoolean alive = new AtomicBoolean(true);

    private static final int PING_PONG_TYPE = 0x45;
    long pingAckTimeout;
    protected ScheduledFuture<?> pingAckTimeoutFuture;

    public TripleHttp3PingPongHandler(long pingAckTimeout) {
        this.pingAckTimeout = pingAckTimeout;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        QuicStreamChannel streamChannel = Http3.getLocalControlStream(ctx.channel());
        Optional.ofNullable(streamChannel).ifPresent(channel -> sendPingFrame(ctx, streamChannel));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        sendPingFrame(ctx);
        System.out.println(msg.getClass());
        if (msg instanceof Http3UnknownFrame) {
            Http3UnknownFrame http3UnknownFrame = (Http3UnknownFrame)msg;
            if (http3UnknownFrame.type() == PING_PONG_TYPE) {
                sendPingFrame(ctx);
            }
        }
        if (msg instanceof Http3GoAwayFrame) {
            if (!alive.get()) {
                ctx.fireUserEventTriggered(new DefaultHttp3GoAwayFrame(123));
            }
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        Optional.ofNullable(pingAckTimeoutFuture).ifPresent(future -> future.cancel(true));
        pingAckTimeoutFuture = null;
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        alive.set(false);
    }

    private void sendPingFrame(ChannelHandlerContext ctx) {
            sendPingFrame(ctx, ctx.channel());
    }

    private void sendPingFrame(ChannelHandlerContext ctx, Channel controlStream) {
        if (alive.get()) {
            pingAckTimeoutFuture = ctx.executor().schedule(new HealthCheckChannelTask(ctx, controlStream, alive),
                pingAckTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private static class HealthCheckChannelTask implements Runnable {

        private final ChannelHandlerContext ctx;
        private final AtomicBoolean alive;
        private final Channel controlStream;
        public HealthCheckChannelTask(ChannelHandlerContext ctx,Channel controlStream, AtomicBoolean alive) {
            this.ctx = ctx;
            this.alive = alive;
            this.controlStream = controlStream;
        }

        @Override
        public void run() {
            Optional.ofNullable(controlStream).ifPresent(channel -> {
                try {
                    Http3UnknownFrame frame = new DefaultHttp3UnknownFrame(PING_PONG_TYPE, ctx.alloc().buffer(0));
                    channel.writeAndFlush(frame).addListener(future -> {
                        if (!future.isSuccess()) {
                            alive.compareAndSet(true, false);
                            ctx.close();
                        }
                        System.out.println("ping-pong");
                    });
                } catch (Exception e) {
                e.printStackTrace();
                }
            });
        }
    }

}
