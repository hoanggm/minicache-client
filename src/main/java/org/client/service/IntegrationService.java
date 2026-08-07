package org.client.service;

import org.client.annotation.ReadCommand;
import org.client.annotation.WriteCommand;
import org.client.collapser.SingleFlightCollapsingHandler;
import org.client.exception.IntegrationException;
import org.client.helper.Command;
import org.client.pool.MiniCacheCluster;

public class IntegrationService {
    private final MiniCacheCluster clusterClient;
    private final SingleFlightCollapsingHandler collapsingHandler;

    public IntegrationService(MiniCacheCluster clusterClient) {
        this.clusterClient = clusterClient;
        this.collapsingHandler = null;
    }

    public IntegrationService(MiniCacheCluster clusterClient,
                              SingleFlightCollapsingHandler collapsingHandler) {
        this.clusterClient = clusterClient;
        this.collapsingHandler = collapsingHandler;
    }

    @ReadCommand
    public String get(String key) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s", Command.GET.name(), key),
                    () -> this.doGet(key)
            );
        } else {
            return this.doGet(key);
        }
    }

    private String doGet(String key) {
        try {
            return clusterClient.get(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GET", e);
        }
    }

    @WriteCommand
    public String set(String key, String value) {
        try {
            return clusterClient.set(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    @WriteCommand
    public String set(String key, String value, Boolean notExist) {
        try {
            return clusterClient.set(key, value, notExist);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    @WriteCommand
    public String set(String key, String value, Boolean notExist, Integer timeToLive) {
        try {
            return clusterClient.set(key, value, notExist, timeToLive);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    @WriteCommand
    public Boolean del(String key) {
        try {
            return clusterClient.del(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_DEL", e);
        }
    }

    @ReadCommand
    public Boolean exists(String key) {
        if (this.collapsingHandler != null) {
            var result = this.collapsingHandler.execute(
                    String.format("%s:%s", Command.EXISTS.name(), key),
                    () -> String.valueOf(this.doExists(key))
            );
            return Boolean.valueOf(result);
        } else {
            return this.doExists(key);
        }
    }

    private Boolean doExists(String key) {
        try {
            return clusterClient.exists(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_EXISTS", e);
        }
    }

    @ReadCommand
    public Boolean bfExists(String key, String value) {
        if (this.collapsingHandler != null) {
            var result = this.collapsingHandler.execute(
                    String.format("%s:%s", Command.BF_EXISTS.name(), key),
                    () -> String.valueOf(this.doBfExists(key, value))
            );
            return Boolean.valueOf(result);
        } else {
            return this.doBfExists(key, value);
        }
    }

    private Boolean doBfExists(String key, String value) {
        try {
            return clusterClient.bfExist(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_EXISTS", e);
        }
    }

    @WriteCommand
    public String bfInit(String key, Integer expectedElements, Double falsePositive) {
        try {
            return clusterClient.bfInit(key, expectedElements, falsePositive);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_INIT", e);
        }
    }

    @WriteCommand
    public String bfAdd(String key, String value) {
        try {
            return clusterClient.bfAdd(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_ADD", e);
        }
    }

    @WriteCommand
    public Boolean bfRm(String key) {
        try {
            return clusterClient.bfRm(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_RM", e);
        }
    }

    @WriteCommand
    public Boolean bfRs(String key) {
        try {
            return clusterClient.bfRs(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_RS", e);
        }
    }

    @ReadCommand
    public String zScore(String key, String member) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.Z_SCR.name(), key, member),
                    () -> this.doZScore(key, member)
            );
        } else {
            return this.doZScore(key, member);
        }
    }

    private String doZScore(String key, String member) {
        try {
            return clusterClient.zScore(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_SCR", e);
        }
    }

    @WriteCommand
    public String zAdd(String key, Double score, String member, String value) {
        try {
            return clusterClient.zAdd(key, score, member, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_ADD", e);
        }
    }

    @WriteCommand
    public Boolean zRem(String key, String member) {
        try {
            return clusterClient.zRem(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RM", e);
        }
    }

    @WriteCommand
    public Boolean zDel(String key) {
        try {
            return clusterClient.zDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_DEL", e);
        }
    }

    @ReadCommand
    public String zRank(String key, String member) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.Z_RANK.name(), key, member),
                    () -> this.doZRank(key, member)
            );
        } else {
            return this.doZRank(key, member);
        }
    }

    private String doZRank(String key, String member) {
        try {
            return clusterClient.zRank(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RANK", e);
        }
    }

    @ReadCommand
    public String zRangeByPositions(String key, Integer start, Integer stop) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%d:%d", Command.Z_RANGE.name(), key, start, stop),
                    () -> this.doZRangeByPositions(key, start, stop)
            );
        } else {
            return this.doZRangeByPositions(key, start, stop);
        }
    }

    private String doZRangeByPositions(String key, Integer start, Integer stop) {
        try {
            return clusterClient.zRangeByPositions(key, start, stop);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RAN", e);
        }
    }

    @ReadCommand
    public String zRangeByScore(String key, Double minScore, Double maxScore) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%f:%f", Command.Z_RSCR.name(), key, minScore, maxScore),
                    () -> this.doZRangeByScore(key, minScore, maxScore)
            );
        } else {
            return this.doZRangeByScore(key, minScore, maxScore);
        }
    }

    private String doZRangeByScore(String key, Double minScore, Double maxScore) {
        try {
            return clusterClient.zRangeByScore(key, minScore, maxScore);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RSCR", e);
        }
    }

    @ReadCommand
    public String zTop(String key, Integer top) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%d", Command.Z_TOP.name(), key, top),
                    () -> this.doZTop(key, top)
            );
        } else {
            return this.doZTop(key, top);
        }
    }

    private String doZTop(String key, Integer top) {
        try {
            return clusterClient.zTop(key, top);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_TOP", e);
        }
    }

    @WriteCommand
    public Boolean zIncrBy(String key, Double increment, String member) {
        try {
            return clusterClient.zIncrBy(key, increment, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_INCR", e);
        }
    }

    @ReadCommand
    public String zGetByPosition(String key, Integer position) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%d", Command.Z_POS.name(), key, position),
                    () -> this.doZGetByPosition(key, position)
            );
        } else {
            return this.doZGetByPosition(key, position);
        }
    }

    private String doZGetByPosition(String key, Integer position) {
        try {
            return clusterClient.zGetByPosition(key, position);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_POS", e);
        }
    }

    @WriteCommand
    public String geoAdd(String key, String member, Double lat, Double lon) {
        try {
            return clusterClient.geoAdd(key, member, lat, lon);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_ADD", e);
        }
    }

    @ReadCommand
    public String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%f:%f:%f:%d", Command.GEO_SEARCH.name(), key, centerLat, centerLon, radiusMeters, limit),
                    () -> this.doGeoSearch(key, centerLat, centerLon, radiusMeters, limit)
            );
        } else {
            return this.doGeoSearch(key, centerLat, centerLon, radiusMeters, limit);
        }
    }

    private String doGeoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        try {
            return clusterClient.geoSearch(key, centerLat, centerLon, radiusMeters, limit);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_SEARCH", e);
        }
    }

    @ReadCommand
    public String geoDist(String key, String member1, String member2) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s:%s", Command.GEO_DIST.name(), key, member1, member2),
                    () -> this.doGeoDist(key, member1, member2)
            );
        } else {
            return this.doGeoDist(key, member1, member2);
        }
    }

    private String doGeoDist(String key, String member1, String member2) {
        try {
            return clusterClient.geoDist(key, member1, member2);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_DIST", e);
        }
    }

    @WriteCommand
    public Boolean geoDel(String key) {
        try {
            return clusterClient.geoDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_DEL", e);
        }
    }

    @WriteCommand
    public Boolean geoRm(String key, String member) {
        try {
            return clusterClient.geoRm(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_RM", e);
        }
    }

    @ReadCommand
    public String geoGet(String key, String member) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.GEO_GET.name(), key, member),
                    () -> this.doGeoGet(key, member)
            );
        } else {
            return this.doGeoGet(key, member);
        }
    }

    private String doGeoGet(String key, String member) {
        try {
            return clusterClient.geoGet(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_GET", e);
        }
    }

    @ReadCommand
    public String geoNb(String key, String member) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.GEO_NB.name(), key, member),
                    () -> this.doGeoNb(key, member)
            );
        } else {
            return this.doGeoNb(key, member);
        }
    }

    private String doGeoNb(String key, String member) {
        try {
            return clusterClient.geoNb(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_NB", e);
        }
    }

    @ReadCommand
    public Boolean geoExists(String key, String member) {
        if (this.collapsingHandler != null) {
            var result = this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.GEO_EXISTS.name(), key, member),
                    () -> String.valueOf(this.doGeoExists(key, member))
            );
            return Boolean.valueOf(result);
        } else {
            return this.doGeoExists(key, member);
        }
    }

    private Boolean doGeoExists(String key, String member) {
        try {
            return clusterClient.geoExists(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_EXISTS", e);
        }
    }

    @ReadCommand
    public String geoEncode(String key, String member) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.GEO_ENCODE.name(), key, member),
                    () -> this.doGeoEncode(key, member)
            );
        } else {
            return this.doGeoEncode(key, member);
        }
    }

    private String doGeoEncode(String key, String member) {
        try {
            return clusterClient.geoEncode(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_ENCODE", e);
        }
    }

    @WriteCommand
    public String hSet(String key, String field, String value) {
        try {
            return clusterClient.hSet(key, field, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_SET", e);
        }
    }

    @ReadCommand
    public String hGet(String key, String field) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.H_GET.name(), key, field),
                    () -> this.doHGet(key, field)
            );
        } else {
            return this.doHGet(key, field);
        }
    }

    private String doHGet(String key, String field) {
        try {
            return clusterClient.hGet(key, field);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_GET", e);
        }
    }

    @ReadCommand
    public String hGetAll(String key) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s", Command.H_ALL.name(), key),
                    () -> this.doHGetAll(key)
            );
        } else {
            return this.doHGetAll(key);
        }
    }

    private String doHGetAll(String key) {
        try {
            return clusterClient.hGetAll(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_GET_ALL", e);
        }
    }

    @WriteCommand
    public Boolean hDel(String key) {
        try {
            return clusterClient.hDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_DEL", e);
        }
    }

    @WriteCommand
    public Boolean hRm(String key, String field) {
        try {
            return clusterClient.hRm(key, field);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_RM", e);
        }
    }
}