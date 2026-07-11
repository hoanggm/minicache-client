package org.client.pool;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.client.helper.ClusterAction;
import org.client.helper.Constant;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MiniCacheCluster implements AutoCloseable {
    private final Map<String, MiniCachePool> nodePools;
    private final List<MiniCachePool> availablePools;
    private final Boolean isStrictMode;
    private final CircuitBreaker circuitBreaker;
    private String currentLeaderNodeId;

    public MiniCacheCluster(String clusterNodesStr, int coreConnections,
                            int maxConnections, boolean isStrictMode,
                            int queuingTime, int connectTimeOut,
                            int readTimeOut, int bufferSize) {
        this.nodePools = new HashMap<>();
        this.availablePools = new ArrayList<>();
        this.isStrictMode = isStrictMode;
        this.circuitBreaker = CircuitBreaker.of("minicache-cb", CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slowCallRateThreshold(50)
                .slowCallDurationThreshold(Duration.ofSeconds(2))
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .slidingWindowSize(20)
                .recordExceptions(IOException.class, RuntimeException.class)
                .build());

        String[] nodes = clusterNodesStr.split(",");
        for (String node : nodes) {
            String[] parts = node.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            // Mỗi một node vật lý quản lý bởi một pool kết nối độc lập
            MiniCachePool pool = new MiniCachePool(host, port, coreConnections, maxConnections,
                    queuingTime, connectTimeOut, readTimeOut, bufferSize);
            nodePools.put(node.trim(), pool);

            if (currentLeaderNodeId == null) {
                currentLeaderNodeId = node.trim();
            }
        }
        refreshLeaderDiscovery();

        this.availablePools.clear();
        this.availablePools.addAll(nodePools.values());
    }

    /**
     * Hàm dùng để cô lập logic fallback khi không tìm thấy bất kỳ node nào phù hợp
     */
    private MiniCachePool getFallbackPool() {
        if (!nodePools.isEmpty()) {
            return nodePools.values().iterator().next();
        }
        throw new IllegalStateException();
    }

    /**
     * Lấy pool dựa trên vai trò thao tác dữ liệu
     */
    private MiniCachePool getPool(boolean isWrite) {
        MiniCachePool leaderPool = (currentLeaderNodeId != null) ? nodePools.get(currentLeaderNodeId) : null;

        if (this.isStrictMode) {
            return (leaderPool != null) ? leaderPool : getFallbackPool();
        }

        if (isWrite || leaderPool == null) {
            return (leaderPool != null) ? leaderPool : getFallbackPool();
        }

        int size = availablePools.size();
        if (size == 0) {
            return getFallbackPool();
        }
        if (size == 1) {
            return availablePools.get(0);
        }

        int randomIndex = ThreadLocalRandom.current().nextInt(size);
        return availablePools.get(randomIndex);
    }

    /**
     * Cơ chế quét tìm Leader (Leader Discovery) bằng cách hỏi thăm tuần tự các Node
     */
    public synchronized void refreshLeaderDiscovery() {
        for (String node : nodePools.keySet()) {
            try (MiniCacheClient client = nodePools.get(node).getResource()) {
                String pingResponse = client.ping();
                if (Constant.LEADER.equalsIgnoreCase(pingResponse) || Constant.PONG.equalsIgnoreCase(pingResponse)) {
                    this.currentLeaderNodeId = node;
                    return;
                }
            } catch (Exception ignored) {
            }
        }
    }

    public String doExecute(ClusterAction action, boolean isWrite) throws IOException {
        try {
            return circuitBreaker.executeCheckedSupplier(() -> {
                int retries = this.nodePools.size();
                if (retries == 0) {
                    retries = 3;
                }

                while (retries > 0) {
                    MiniCachePool targetPool = getPool(isWrite);
                    if (targetPool == null) {
                        refreshLeaderDiscovery();
                        retries--;
                        continue;
                    }

                    try (MiniCacheClient client = targetPool.getResource()) {
                        return action.execute(client);
                    } catch (Exception e) {
                        retries--;

                        // Chỉ chủ động kích hoạt quét lại cụm (Leader Discovery) ở những lượt thử cuối
                        if (retries == 1 || retries == 0) {
                            refreshLeaderDiscovery();
                        }

                        if (retries == 0) {
                            throw new IOException("Cluster unavailable after maximizing retries. Last error: " + e.getMessage(), e);
                        }
                    }
                }
                throw new IOException("Failed to execute cluster command due to empty routing path");
            });
        } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
            throw new IOException("Circuit Breaker OPEN", e);
        } catch (Throwable t) {
            if (t instanceof IOException) throw (IOException) t;
            throw new IOException("Cluster Error", t);
        }
    }

    public String set(String key, String value) throws IOException {
        return doExecute(c -> c.set(key, value), true);
    }

    public String set(String key, String value, Boolean notExist) throws IOException {
        return doExecute(c -> c.set(key, value, notExist), true);
    }

    public String set(String key, String value, Boolean notExist, Integer timeToLive) throws IOException {
        return doExecute(c -> c.set(key, value, notExist, timeToLive), true);
    }

    public String get(String key) throws IOException {
        return doExecute(c -> c.get(key), false);
    }

    public boolean exists(String key) throws IOException {
        String res = doExecute(c -> c.exists(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                false);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public boolean del(String key) throws IOException {
        String res = doExecute(c -> c.del(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public String bfInit(String key, Integer expectedElements, Double falsePositive) throws IOException {
        return doExecute(c -> c.bfInit(key, expectedElements, falsePositive), true);
    }

    public String bfAdd(String key, String value) throws IOException {
        return doExecute(c -> c.bfAdd(key, value), true);
    }

    public boolean bfExist(String key, String value) throws IOException {
        String res = doExecute(c -> c.bfExist(key, value)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                false);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public String zAdd(String key, Double score, String member, String value) throws IOException {
        return doExecute(c -> c.zAdd(key, score, member, value), true);
    }

    public boolean zRem(String key, String member) throws IOException {
        String res = doExecute(c -> c.zRem(key, member)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public boolean zDel(String key) throws IOException {
        String res = doExecute(c -> c.zDel(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public String zScore(String key, String member) throws IOException {
        return doExecute(c -> c.zScore(key, member), false);
    }

    public String zRank(String key, String member) throws IOException {
        return doExecute(c -> c.zRank(key, member), false);
    }

    public String zRangeByPositions(String key, Integer start, Integer stop) throws IOException {
        return doExecute(c -> c.zRangeByPositions(key, start, stop), false);
    }

    public String zGetByPosition(String key, Integer position) throws IOException {
        return doExecute(c -> c.zGetByPosition(key, position), false);
    }

    public String zRangeByScore(String key, Double minScore, Double maxScore) throws IOException {
        return doExecute(c -> c.zRangeByScore(key, minScore, maxScore), false);
    }

    public Boolean zIncrBy(String key, Double increment, String member) throws IOException {
        var res = doExecute(c -> c.zIncrBy(key, increment, member)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    @Override
    public void close() {
        for (MiniCachePool pool : nodePools.values()) {
            try {
                pool.close();
            } catch (Exception ignored) {
            }
        }
    }
}