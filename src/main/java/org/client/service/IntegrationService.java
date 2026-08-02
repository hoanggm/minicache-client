package org.client.service;

import org.client.exception.IntegrationException;
import org.client.pool.MiniCacheCluster;

public class IntegrationService {
    private final MiniCacheCluster clusterClient;

    public IntegrationService(MiniCacheCluster clusterClient) {
        this.clusterClient = clusterClient;
    }

    public String get(String key) {
        try {
            return clusterClient.get(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GET", e);
        }
    }

    public String set(String key, String value) {
        try {
            return clusterClient.set(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    public String set(String key, String value, Boolean notExist) {
        try {
            return clusterClient.set(key, value, notExist);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    public String set(String key, String value, Boolean notExist, Integer timeToLive) {
        try {
            return clusterClient.set(key, value, notExist, timeToLive);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    public Boolean del(String key) {
        try {
            return clusterClient.del(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_DEL", e);
        }
    }

    public Boolean exists(String key) {
        try {
            return clusterClient.exists(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_EXISTS", e);
        }
    }

    public Boolean bfExists(String key, String value) {
        try {
            return clusterClient.bfExist(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_EXISTS", e);
        }
    }

    public String bfInit(String key, Integer expectedElements, Double falsePositive) {
        try {
            return clusterClient.bfInit(key, expectedElements, falsePositive);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_INIT", e);
        }
    }

    public String bfAdd(String key, String value) {
        try {
            return clusterClient.bfAdd(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_ADD", e);
        }
    }

    public Boolean bfRm(String key) {
        try {
            return clusterClient.bfRm(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_RM", e);
        }
    }

    public Boolean bfRs(String key) {
        try {
            return clusterClient.bfRs(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_RS", e);
        }
    }

    public String zScore(String key, String member) {
        try {
            return clusterClient.zScore(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_SCR", e);
        }
    }

    public String zAdd(String key, Double score, String member, String value) {
        try {
            return clusterClient.zAdd(key, score, member, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_ADD", e);
        }
    }

    public Boolean zRem(String key, String member) {
        try {
            return clusterClient.zRem(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RM", e);
        }
    }

    public Boolean zDel(String key) {
        try {
            return clusterClient.zDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_DEL", e);
        }
    }

    public String zRank(String key, String member) {
        try {
            return clusterClient.zRank(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RANK", e);
        }
    }

    public String zRangeByPositions(String key, Integer start, Integer stop) {
        try {
            return clusterClient.zRangeByPositions(key, start, stop);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RAN", e);
        }
    }

    public String zRangeByScore(String key, Double minScore, Double maxScore) {
        try {
            return clusterClient.zRangeByScore(key, minScore, maxScore);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RSCR", e);
        }
    }

    public String zTop(String key, Integer top) {
        try {
            return clusterClient.zTop(key, top);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_TOP", e);
        }
    }

    public Boolean zIncrBy(String key, Double increment, String member) {
        try {
            return clusterClient.zIncrBy(key, increment, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_INCR", e);
        }
    }

    public String zGetByPosition(String key, Integer position) {
        try {
            return clusterClient.zGetByPosition(key, position);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_POS", e);
        }
    }

    public String geoAdd(String key, String member, Double lat, Double lon) {
        try {
            return clusterClient.geoAdd(key, member, lat, lon);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_ADD", e);
        }
    }

    public String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        try {
            return clusterClient.geoSearch(key, centerLat, centerLon, radiusMeters, limit);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_SEARCH", e);
        }
    }

    public String geoDist(String key, String member1, String member2) {
        try {
            return clusterClient.geoDist(key, member1, member2);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_DIST", e);
        }
    }

    public Boolean geoDel(String key) {
        try {
            return clusterClient.geoDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_DEL", e);
        }
    }

    public Boolean geoRm(String key, String member) {
        try {
            return clusterClient.geoRm(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_RM", e);
        }
    }

    public String geoGet(String key, String member) {
        try {
            return clusterClient.geoGet(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_GET", e);
        }
    }

    public String geoNb(String key, String member) {
        try {
            return clusterClient.geoNb(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_NB", e);
        }
    }

    public Boolean geoExists(String key, String member) {
        try {
            return clusterClient.geoExists(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_EXISTS", e);
        }
    }

    public String geoEncode(String key, String member) {
        try {
            return clusterClient.geoEncode(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_ENCODE", e);
        }
    }

    public String hSet(String key, String field, String value) {
        try {
            return clusterClient.hSet(key, field, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_SET", e);
        }
    }

    public String hGet(String key, String field) {
        try {
            return clusterClient.hGet(key, field);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_GET", e);
        }
    }

    public String hGetAll(String key) {
        try {
            return clusterClient.hGetAll(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_GET_ALL", e);
        }
    }

    public Boolean hDel(String key) {
        try {
            return clusterClient.hDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_DEL", e);
        }
    }

    public Boolean hRm(String key, String field) {
        try {
            return clusterClient.hRm(key, field);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_RM", e);
        }
    }
}