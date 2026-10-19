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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MiniCacheCluster implements AutoCloseable {
    private final Map<String, MiniCachePool> nodePools;
    private final List<MiniCachePool> availablePools;
    private final Boolean isStrictMode;
    private String currentLeaderNodeId;
    private final Map<String, CircuitBreaker> nodeCircuitBreakers;
    private final ReentrantReadWriteLock rwLock;

    public MiniCacheCluster(String clusterNodesStr, int coreConnections,
                            int maxConnections, boolean isStrictMode,
                            int queuingTime, int connectTimeOut,
                            int readTimeOut, int bufferSize) {
        this.nodePools = new HashMap<>();
        this.availablePools = new ArrayList<>();
        this.isStrictMode = isStrictMode;
        this.nodeCircuitBreakers = new HashMap<>();
        this.rwLock = new ReentrantReadWriteLock();

        String[] nodes = clusterNodesStr.split(",");
        for (String node : nodes) {
            String[] parts = node.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            // Mỗi một node vật lý quản lý bởi một pool kết nối độc lập
            MiniCachePool pool = new MiniCachePool(host, port, coreConnections, maxConnections,
                    queuingTime, connectTimeOut, readTimeOut, bufferSize, node.trim());
            nodePools.put(node.trim(), pool);

            if (currentLeaderNodeId == null) {
                currentLeaderNodeId = node.trim();
            }

            CircuitBreaker cb = CircuitBreaker.of(node.trim(),
                    CircuitBreakerConfig.custom()
                            .failureRateThreshold(50)
                            .slowCallRateThreshold(50)
                            .slowCallDurationThreshold(Duration.ofSeconds(1))
                            .waitDurationInOpenState(Duration.ofSeconds(10))
                            .slidingWindowSize(10)
                            .build());
            cb.getEventPublisher().onStateTransition(event -> this.onCircuitBreakerStateTransition(event, cb));
            nodeCircuitBreakers.put(node.trim(), cb);
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
        this.nodeCircuitBreakers = new HashMap<>();
        this.rwLock = new ReentrantReadWriteLock();

        String[] nodes = clusterNodesStr.split(",");
        for (String node : nodes) {
            String[] parts = node.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            // Mỗi một node vật lý quản lý bởi một pool kết nối độc lập
            MiniCachePool pool = new MiniCachePool(host, port, coreConnections, maxConnections,
                    queuingTime, connectTimeOut, readTimeOut, bufferSize,
                    authModel.username(), authModel.password(), node.trim());
            nodePools.put(node.trim(), pool);

            if (currentLeaderNodeId == null) {
                currentLeaderNodeId = node.trim();
            }

            CircuitBreaker cb = CircuitBreaker.of(node.trim(),
                    CircuitBreakerConfig.custom()
                            .failureRateThreshold(50)
                            .slowCallRateThreshold(50)
                            .slowCallDurationThreshold(Duration.ofSeconds(1))
                            .waitDurationInOpenState(Duration.ofSeconds(10))
                            .slidingWindowSize(10)
                            .build());
            cb.getEventPublisher().onStateTransition(event -> this.onCircuitBreakerStateTransition(event, cb));
            nodeCircuitBreakers.put(node.trim(), cb);
        }
        refreshLeaderDiscovery();

        this.availablePools.clear();
        this.availablePools.addAll(nodePools.values());
    }

    /**
     * Hàm thực thi khi CircuitBreaker chuyển đổi trạng thái
     */
    private void onCircuitBreakerStateTransition(io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent event,
                                                 CircuitBreaker cb) {
        String transition = event.getStateTransition().toString();
        String nodeId = cb.getName();
        if (transition.contains("TO_OPEN")) {
            rwLock.writeLock().lock();
            try {
                for (MiniCachePool mncPool : availablePools) {
                    if (mncPool.getNodeId().equals(nodeId)) {
                        mncPool.changeState(MiniCachePool.POOL_STATE.NOT_AVAILABLE);
                    }
                }
            } finally {
                rwLock.writeLock().unlock();
            }
            refreshLeaderDiscovery();
        } else if (transition.contains("TO_CLOSED")) {
            rwLock.writeLock().lock();
            try {
                for (MiniCachePool mncPool : availablePools) {
                    if (mncPool.getNodeId().equals(nodeId)) {
                        mncPool.changeState(MiniCachePool.POOL_STATE.AVAILABLE);
                    }
                }
            } finally {
                rwLock.writeLock().unlock();
            }
            refreshLeaderDiscovery();
        }
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
        var pool = availablePools.stream()
                .filter(x -> x.getState().equals(MiniCachePool.POOL_STATE.AVAILABLE))
                .toList();
        MiniCachePool leaderPool = (currentLeaderNodeId != null) ? nodePools.get(currentLeaderNodeId) : null;

        if (this.isStrictMode) {
            return (leaderPool != null) ? leaderPool : getFallbackPool();
        }

        if (isWrite || leaderPool == null) {
            return (leaderPool != null) ? leaderPool : getFallbackPool();
        }

        int size = pool.size();
        if (size == 0) {
            return getFallbackPool();
        }
        if (size == 1) {
            return pool.get(0);
        }

        int randomIndex = ThreadLocalRandom.current().nextInt(size);
        return pool.get(randomIndex);
    }

    /**
     * Cơ chế quét tìm Leader (Leader Discovery) bằng cách hỏi thăm tuần tự các Node
     */
    public void refreshLeaderDiscovery() {
        rwLock.writeLock().lock();
        try {
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
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public String doExecute(ClusterAction action, boolean isWrite) throws IOException {
        int retries = this.nodePools.size();
        Throwable lastException = null;

        while (retries > 0) {
            MiniCachePool targetPool = getPool(isWrite);
            String targetNodeKey = targetPool.getNodeId();
            CircuitBreaker cb = nodeCircuitBreakers.get(targetNodeKey);

            try {
                return cb.executeCheckedSupplier(() -> {
                    try (MiniCacheClient client = targetPool.getResource()) {
                        return action.execute(client);
                    }
                });
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                lastException = e;
                retries--;
            } catch (Throwable t) {
                lastException = t;
                retries--;
                refreshLeaderDiscovery();
            }
        }
        throw new IOException("Cluster unavailable after retries", lastException);
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