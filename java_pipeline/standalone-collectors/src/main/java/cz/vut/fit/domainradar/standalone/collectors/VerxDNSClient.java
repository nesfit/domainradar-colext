package cz.vut.fit.domainradar.standalone.collectors;

/*
 * Copyright (c) 2011-2023 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

import io.netty.channel.*;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.dns.*;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;
import io.netty.util.internal.ThreadLocalRandom;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.impl.PartialPooledByteBufAllocator;
import io.vertx.core.dns.*;
import io.vertx.core.dns.DnsResponseCode;
import io.vertx.core.dns.impl.decoder.RecordDecoder;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.impl.ConnectionBase;
import org.jetbrains.annotations.Nullable;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class VerxDNSClient {

    private static final char[] HEX_TABLE = "0123456789abcdef".toCharArray();

    private final VertxInternal vertx;
    private final LongObjectMap<Query> inProgressMap = new LongObjectHashMap<>();
    private final InetSocketAddress dnsServer;
    private final ContextInternal context;
    private final DatagramChannel channel;
    private final DnsClientOptions options;
    private volatile Future<Void> closed;

    public VerxDNSClient(VertxInternal vertx, DnsClientOptions options) {
        Objects.requireNonNull(options, "no null options accepted");
        Objects.requireNonNull(options.getHost(), "no null host accepted");

        this.options = new DnsClientOptions(options);

        this.dnsServer = new InetSocketAddress(options.getHost(), options.getPort());
        if (this.dnsServer.isUnresolved()) {
            throw new IllegalArgumentException("Cannot resolve the host to a valid ip address");
        }
        this.vertx = vertx;

        var transport = vertx.transport();
        context = vertx.getOrCreateContext();
        channel = transport.datagramChannel(this.dnsServer.getAddress() instanceof Inet4Address ? InternetProtocolFamily.IPv4 : InternetProtocolFamily.IPv6);
        channel.config().setOption(ChannelOption.DATAGRAM_CHANNEL_ACTIVE_ON_REGISTRATION, true);
        MaxMessagesRecvByteBufAllocator bufAllocator = channel.config().getRecvByteBufAllocator();
        bufAllocator.maxMessagesPerRead(1);
        channel.config().setAllocator(PartialPooledByteBufAllocator.INSTANCE);
        context.nettyEventLoop().register(channel);
        channel.pipeline().addLast(new DatagramDnsQueryEncoder());
        channel.pipeline().addLast(new DatagramDnsResponseDecoder());
        channel.pipeline().addLast(new SimpleChannelInboundHandler<DnsResponse>() {

            protected void channelRead0(ChannelHandlerContext ctx, DnsResponse msg) {
                DefaultDnsQuestion question = msg.recordAt(DnsSection.QUESTION);
                Query query = inProgressMap.get(dnsMessageId(msg.id(), question.name()));
                if (query != null) {
                    query.handle(msg);
                }
            }
        });
    }

    public Future<List<TTLTuple<String>>> resolveIPs(String name) {
        return lookup(name, DnsRecordType.A, DnsRecordType.AAAA);
    }

    public Future<List<TTLTuple<String>>> resolveA(String name) {
        return lookup(name, DnsRecordType.A);
    }

    public Future<List<TTLTuple<MxRecord>>> resolveMX(String name) {
        return lookup(name, DnsRecordType.MX);
    }

    public Future<List<TTLTuple<String>>> resolveTXT(String name) {
        return lookup(name, DnsRecordType.TXT);
    }

    public Future<List<TTLTuple<String>>> resolveCNAME(String name) {
        return lookup(name, DnsRecordType.CNAME);
    }

    @SuppressWarnings("unchecked")
    private <T> Future<List<TTLTuple<T>>> lookup(String name, DnsRecordType... types) {
        ContextInternal ctx = vertx.getOrCreateContext();
        if (closed != null) {
            return ctx.failedFuture(ConnectionBase.CLOSED_EXCEPTION);
        }
        PromiseInternal<List<TTLTuple<T>>> promise = ctx.promise();
        Objects.requireNonNull(name, "no null name accepted");
        EventLoop el = context.nettyEventLoop();
        Query query = new Query(name, types);
        query.promise.addListener(promise);
        if (el.inEventLoop()) {
            query.run();
        } else {
            el.execute(query::run);
        }
        return promise.future();
    }

    private long dnsMessageId(int id, String query) {
        return ((long) query.hashCode() << 16) + (id & 65535);
    }

    public record TTLTuple<T>(long ttl, T value) {
    }

    private class Query<T> {

        final DatagramDnsQuery msg;
        final io.netty.util.concurrent.Promise<List<TTLTuple<T>>> promise;
        final String name;
        final DnsRecordType[] types;
        long timerID;

        public Query(String name, DnsRecordType[] types) {
            this.msg = new DatagramDnsQuery(null, dnsServer, ThreadLocalRandom.current().nextInt())
                    .setRecursionDesired(options.isRecursionDesired());
            if (!name.endsWith(".")) {
                name += ".";
            }
            for (DnsRecordType type : types) {
                msg.addRecord(DnsSection.QUESTION, new DefaultDnsQuestion(name, type, DnsRecord.CLASS_IN));
            }
            this.promise = context.nettyEventLoop().newPromise();
            this.types = types;
            this.name = name;
        }

        private void fail(Throwable cause) {
            inProgressMap.remove(dnsMessageId(msg.id(), name));
            if (timerID >= 0) {
                vertx.cancelTimer(timerID);
            }
            promise.setFailure(cause);
        }

        void handle(DnsResponse msg) {
            DnsResponseCode code = DnsResponseCode.valueOf(msg.code().intValue());
            if (code == DnsResponseCode.NOERROR) {
                inProgressMap.remove(dnsMessageId(msg.id(), name));
                if (timerID >= 0) {
                    vertx.cancelTimer(timerID);
                }
                int count = msg.count(DnsSection.ANSWER);

                List<TTLTuple<T>> records = new ArrayList<>(count);
                for (int idx = 0; idx < count; idx++) {
                    DnsRecord recordRaw = msg.recordAt(DnsSection.ANSWER, idx);

                    T record;
                    try {
                        record = RecordDecoder.decode(recordRaw);
                    } catch (DecoderException e) {
                        fail(e);
                        return;
                    }

                    if (isRequestedType(recordRaw.type(), types)) {
                        records.add(new TTLTuple<>(recordRaw.timeToLive(), record));
                    }
                }

                promise.setSuccess(records);
            } else {
                fail(new DnsException(code));
            }
        }

        void run() {
            inProgressMap.put(dnsMessageId(msg.id(), name), this);
            timerID = vertx.setTimer(options.getQueryTimeout(), id -> {
                timerID = -1;
                context.runOnContext(v -> {
                    fail(new VertxException("DNS query timeout for " + name));
                });
            });
            channel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    context.emit(future.cause(), this::fail);
                }
            });
        }

        private boolean isRequestedType(DnsRecordType dnsRecordType, DnsRecordType[] types) {
            for (DnsRecordType t : types) {
                if (t.equals(dnsRecordType)) {
                    return true;
                }
            }
            return false;
        }
    }


    public void close(Handler<AsyncResult<Void>> handler) {
        close().onComplete(handler);
    }


    public Future<Void> close() {
        PromiseInternal<Void> promise;
        synchronized (this) {
            if (closed != null) {
                return closed;
            }
            promise = vertx.promise();
            closed = promise.future();
        }
        context.runOnContext(v -> {
            new ArrayList<>(inProgressMap.values()).forEach(query -> {
                query.fail(ConnectionBase.CLOSED_EXCEPTION);
            });
            channel.close().addListener(promise);
        });
        return promise.future();
    }
}