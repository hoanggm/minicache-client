package org.client.pool;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MiniCachePool implements AutoCloseable {
    private final LinkedBlockingQueue<MiniCacheClient> pool;
    private final String host;
    private final int port;
    private final int maxConnections;
    private final AtomicInteger currentConnections;
    private final int queuingTime;
    private volatile boolean isClosed = false;
    private final int clientConnectTimeout;
    private final int clientReadTimeout;
    private final int clientDataBufferSize;

    public MiniCachePool(String host, int port, int coreConnections, int maxConnections,
                         int queuingTime, int clientConnectTimeout, int clientReadTimeout,
                         int bufferSize) {
        this.host = host;
        this.port = port;
        this.maxConnections = maxConnections;
        this.pool = new LinkedBlockingQueue<>(maxConnections);
        this.currentConnections = new AtomicInteger(0);
        this.queuingTime = queuingTime;
        this.clientConnectTimeout = clientConnectTimeout;
        this.clientReadTimeout = clientReadTimeout;
        this.clientDataBufferSize = bufferSize;

        for (int i = 0; i < coreConnections; i++) {
            try {
                MiniCacheClient client = new MiniCacheClient(host, port,
                        clientConnectTimeout, clientReadTimeout, bufferSize, this);
                client.connect();
                if (pool.offer(client)) {
                    currentConnections.incrementAndGet();
                } else {
                    client.destroy();
                }
            } catch (Exception ignored) {
            }
        }
    }

    public MiniCacheClient getResource() {
        if (isClosed) {
            throw new IllegalStateException("Pool Closed");
        }

        MiniCacheClient client = pool.poll();
        if (client != null) {
            return client;
        }

        int currentCount = currentConnections.get();
        if (currentCount < maxConnections) {
            while (currentCount < maxConnections) {
                if (currentConnections.compareAndSet(currentCount, currentCount + 1)) {
                    try {
                        MiniCacheClient newClient = new MiniCacheClient(host, port,
                                clientConnectTimeout, clientReadTimeout, clientDataBufferSize, this);
                        newClient.connect();
                        return newClient;
                    } catch (Exception e) {
                        currentConnections.decrementAndGet();
                        break;
                    }
                }
                currentCount = currentConnections.get();
            }
        }

        try {
            client = pool.poll(this.queuingTime, TimeUnit.MILLISECONDS);
            if (client == null) {
                throw new RuntimeException("Pool Timeout: " + this.queuingTime + " ms)");
            }
            return client;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
    }

    public void returnResource(MiniCacheClient client) {
        if (client == null) return;

        if (isClosed) {
            client.destroy();
            currentConnections.decrementAndGet();
            return;
        }

        boolean returned = pool.offer(client);
        if (!returned) {
            client.destroy();
            currentConnections.decrementAndGet();
        }
    }

    @Override
    public void close() {
        if (isClosed) return;
        synchronized (this) {
            if (isClosed) return;
            this.isClosed = true;
        }

        MiniCacheClient client;
        while ((client = pool.poll()) != null) {
            client.destroy();
            currentConnections.decrementAndGet();
        }
    }
}