package org.client.pool;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.client.helper.ClusterAction;
import org.client.helper.Constant;
import org.client.model.AuthModel;

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

    public MiniCacheCluster(String clusterNodesStr, int coreConnections,
                            int maxConnections, boolean isStrictMode,
                            int queuingTime, int connectTimeOut,
                            int readTimeOut, int bufferSize,
                            AuthModel authModel) {
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
                    queuingTime, connectTimeOut, readTimeOut, bufferSize,
                    authModel.username(), authModel.password());
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

    public boolean bfRm(String key) throws IOException {
        String res = doExecute(c -> c.bfRm(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public boolean bfRs(String key) throws IOException {
        String res = doExecute(c -> c.bfRs(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
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

    public String zTop(String key, Integer top) throws IOException {
        return doExecute(c -> c.zTop(key, top), false);
    }

    public Boolean zIncrBy(String key, Double increment, String member) throws IOException {
        var res = doExecute(c -> c.zIncrBy(key, increment, member)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public String geoAdd(String key, String member, Double lat, Double lon) throws IOException {
        return doExecute(c -> c.geoAdd(key, member, lat, lon), true);
    }

    public String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) throws IOException {
        return doExecute(c -> c.geoSearch(key, centerLat, centerLon, radiusMeters, limit), false);
    }

    public String geoDist(String key, String member1, String member2) throws IOException {
        return doExecute(c -> c.geoDist(key, member1, member2), false);
    }

    public Boolean geoDel(String key) throws IOException {
        var res = doExecute(c -> c.geoDel(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public Boolean geoRm(String key, String member) throws IOException {
        var res = doExecute(c -> c.geoRm(key, member)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public String geoGet(String key, String member) throws IOException {
        return doExecute(c -> c.geoGet(key, member), false);
    }

    public String geoNb(String key, String member) throws IOException {
        return doExecute(c -> c.geoNb(key, member), false);
    }

    public Boolean geoExists(String key, String member) throws IOException {
        var res = doExecute(c -> c.geoExists(key, member)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public String geoEncode(String key, String member) throws IOException {
        return doExecute(c -> c.geoEncode(key, member), false);
    }

    public String hSet(String key, String field, String value) throws IOException {
        return doExecute(c -> c.hSet(key, field, value), true);
    }

    public String hGet(String key, String field) throws IOException {
        return doExecute(c -> c.hGet(key, field), false);
    }

    public String hGetAll(String key) throws IOException {
        return doExecute(c -> c.hGetAll(key), false);
    }

    public Boolean hDel(String key) throws IOException {
        var res = doExecute(c -> c.hDel(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public Boolean hRm(String key, String field) throws IOException {
        var res = doExecute(c -> c.hRm(key, field)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public Boolean fzDel(String key) throws IOException {
        var res = doExecute(c -> c.fzDel(key)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public Boolean fzRm(String key, String word) throws IOException {
        var res = doExecute(c -> c.fzRm(key, word)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public String fzAdd(String key, String word, Long frequency) throws IOException {
        return doExecute(c -> c.fzAdd(key, word, frequency), true);
    }

    public String fzSearch(String key, String query, Integer limit) throws IOException {
        return doExecute(c -> c.fzSearch(key, query, limit), false);
    }

    public String fzSuggest(String key, String query, Integer limit, Integer maxDist) throws IOException {
        return doExecute(c -> c.fzSuggest(key, query, limit, maxDist), false);
    }

    public String fzGetExact(String key, String word) throws IOException {
        return doExecute(c -> c.fzGetExact(key, word), false);
    }

    public String fzRandom(String key, Integer limit) throws IOException {
        return doExecute(c -> c.fzRandom(key, limit), false);
    }

    public String fzPhonetic(String key, String input, Integer limit) throws IOException {
        return doExecute(c -> c.fzPhonetic(key, input, limit), false);
    }

    public Boolean fzIncr(String key, String word, Long incr) throws IOException {
        var res = doExecute(c -> c.fzIncr(key, word, incr)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                true);
        return Constant.IResponse.SUCCESS.equals(res);
    }

    public Boolean fzExists(String key, String word) throws IOException {
        var res = doExecute(c -> c.fzExists(key, word)
                        ? Constant.IResponse.SUCCESS
                        : Constant.IResponse.FAILURE,
                false);
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