package org.client.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.module.blackbird.BlackbirdModule;
import org.client.annotation.ReadCommand;
import org.client.annotation.WriteCommand;
import org.client.collapser.SingleFlightCollapsingHandler;
import org.client.exception.IntegrationException;
import org.client.exception.ParsingException;
import org.client.helper.Command;
import org.client.model.FzSuggestModel;
import org.client.model.FzWordModel;
import org.client.pool.MiniCacheCluster;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IntegrationService {
    private final MiniCacheCluster clusterClient;
    private final SingleFlightCollapsingHandler collapsingHandler;
    private final ObjectMapper objectMapper;

    public IntegrationService(MiniCacheCluster clusterClient) {
        this.clusterClient = clusterClient;
        this.collapsingHandler = null;
        this.objectMapper = JsonMapper.builder()
                .addModule(new BlackbirdModule())
                .build();
    }

    public IntegrationService(MiniCacheCluster clusterClient,
                              SingleFlightCollapsingHandler collapsingHandler) {
        this.clusterClient = clusterClient;
        this.collapsingHandler = collapsingHandler;
        this.objectMapper = JsonMapper.builder()
                .addModule(new BlackbirdModule())
                .build();
    }

    @ReadCommand(Command.GET)
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

    @WriteCommand(Command.PUT)
    public String set(String key, String value) {
        try {
            return clusterClient.set(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    @WriteCommand(Command.PUT)
    public String set(String key, String value, Boolean notExist) {
        try {
            return clusterClient.set(key, value, notExist);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    @WriteCommand(Command.PUT)
    public String set(String key, String value, Boolean notExist, Integer timeToLive) {
        try {
            return clusterClient.set(key, value, notExist, timeToLive);
        } catch (Exception e) {
            throw new IntegrationException("ERR_SET", e);
        }
    }

    @WriteCommand(Command.DELETE)
    public Boolean del(String key) {
        try {
            return clusterClient.del(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_DEL", e);
        }
    }

    @ReadCommand(Command.EXISTS)
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

    @ReadCommand(Command.BF_EXISTS)
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

    @WriteCommand(Command.BF_INIT)
    public String bfInit(String key, Integer expectedElements, Double falsePositive) {
        try {
            return clusterClient.bfInit(key, expectedElements, falsePositive);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_INIT", e);
        }
    }

    @WriteCommand(Command.BF_ADD)
    public String bfAdd(String key, String value) {
        try {
            return clusterClient.bfAdd(key, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_ADD", e);
        }
    }

    @WriteCommand(Command.BF_RM)
    public Boolean bfRm(String key) {
        try {
            return clusterClient.bfRm(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_RM", e);
        }
    }

    @WriteCommand(Command.BF_RS)
    public Boolean bfRs(String key) {
        try {
            return clusterClient.bfRs(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_BF_RS", e);
        }
    }

    @ReadCommand(Command.Z_SCR)
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

    @WriteCommand(Command.Z_ADD)
    public String zAdd(String key, Double score, String member, String value) {
        try {
            return clusterClient.zAdd(key, score, member, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_ADD", e);
        }
    }

    @WriteCommand(Command.Z_RM)
    public Boolean zRem(String key, String member) {
        try {
            return clusterClient.zRem(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_RM", e);
        }
    }

    @WriteCommand(Command.Z_DEL)
    public Boolean zDel(String key) {
        try {
            return clusterClient.zDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_DEL", e);
        }
    }

    @ReadCommand(Command.Z_RANK)
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

    @ReadCommand(Command.Z_RANGE)
    public String[] zRangeByPositionsAsObj(String key, Integer start, Integer stop) {
        String val = this.zRangeByPositions(key, start, stop);
        String[] result;

        if (val == null || val.equals("[]")) {
            result = new String[0];
        } else {
            result = Arrays.stream(val.substring(1, val.length() - 1).split(","))
                    .map(String::trim)
                    .toArray(String[]::new);
        }
        return result;
    }

    @ReadCommand(Command.Z_RANGE)
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

    @ReadCommand(Command.Z_RSCR)
    public String[] zRangeByScoreAsObj(String key, Double minScore, Double maxScore) {
        String val = this.zRangeByScore(key, minScore, maxScore);
        String[] result;

        if (val == null || val.equals("[]")) {
            result = new String[0];
        } else {
            result = Arrays.stream(val.substring(1, val.length() - 1).split(","))
                    .map(String::trim)
                    .toArray(String[]::new);
        }
        return result;
    }

    @ReadCommand(Command.Z_RSCR)
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

    @ReadCommand(Command.Z_TOP)
    public String[] zTopAsObj(String key, Integer top) {
        String val = this.zTop(key, top);
        String[] result;

        if (val == null || val.equals("[]")) {
            result = new String[0];
        } else {
            result = Arrays.stream(val.substring(1, val.length() - 1).split(","))
                    .map(String::trim)
                    .toArray(String[]::new);
        }
        return result;
    }

    @ReadCommand(Command.Z_TOP)
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

    @WriteCommand(Command.Z_INCR)
    public Boolean zIncrBy(String key, Double increment, String member) {
        try {
            return clusterClient.zIncrBy(key, increment, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_Z_INCR", e);
        }
    }

    @ReadCommand(Command.Z_POS)
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

    @WriteCommand(Command.GEO_ADD)
    public String geoAdd(String key, String member, Double lat, Double lon) {
        try {
            return clusterClient.geoAdd(key, member, lat, lon);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_ADD", e);
        }
    }

    @ReadCommand(Command.GEO_SEARCH)
    public String[] geoSearchAsObj(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) {
        String val = this.geoSearch(key, centerLat, centerLon, radiusMeters, limit);
        String[] result;

        if (val == null || val.equals("[]")) {
            result = new String[0];
        } else {
            result = Arrays.stream(val.substring(1, val.length() - 1).split(","))
                    .map(String::trim)
                    .toArray(String[]::new);
        }
        return result;
    }

    @ReadCommand(Command.GEO_SEARCH)
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

    @ReadCommand(Command.GEO_DIST)
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

    @WriteCommand(Command.GEO_DEL)
    public Boolean geoDel(String key) {
        try {
            return clusterClient.geoDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_DEL", e);
        }
    }

    @WriteCommand(Command.GEO_RM)
    public Boolean geoRm(String key, String member) {
        try {
            return clusterClient.geoRm(key, member);
        } catch (Exception e) {
            throw new IntegrationException("ERR_GEO_RM", e);
        }
    }

    @ReadCommand(Command.GEO_GET)
    public double[] geoGetAsObj(String key, String member) {
        String val = this.geoGet(key, member);
        try {
            return objectMapper.readValue(
                    val,
                    double[].class
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.GEO_GET)
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

    @ReadCommand(Command.GEO_NB)
    public Map<String, double[]> geoNbAsObj(String key, String member) {
        String val = this.geoNb(key, member);
        try {
            return objectMapper.readValue(
                    val,
                    new TypeReference<>() {
                    }
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.GEO_NB)
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

    @ReadCommand(Command.GEO_EXISTS)
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

    @ReadCommand(Command.GEO_ENCODE)
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

    @WriteCommand(Command.H_SET)
    public String hSet(String key, String field, String value) {
        try {
            return clusterClient.hSet(key, field, value);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_SET", e);
        }
    }

    @ReadCommand(Command.H_GET)
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

    @ReadCommand(Command.H_ALL)
    public Map<String, Object> hGetAllAsObj(String key) {
        String val = this.hGetAll(key);
        try {
            return objectMapper.readValue(
                    val,
                    new TypeReference<>() {
                    }
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.H_ALL)
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

    @WriteCommand(Command.H_DEL)
    public Boolean hDel(String key) {
        try {
            return clusterClient.hDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_DEL", e);
        }
    }

    @WriteCommand(Command.H_RM)
    public Boolean hRm(String key, String field) {
        try {
            return clusterClient.hRm(key, field);
        } catch (Exception e) {
            throw new IntegrationException("ERR_H_RM", e);
        }
    }

    @WriteCommand(Command.FZ_DEL)
    public Boolean fzDel(String key) {
        try {
            return clusterClient.fzDel(key);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_DEL", e);
        }
    }

    @WriteCommand(Command.FZ_RM)
    public Boolean fzRm(String key, String word) {
        try {
            return clusterClient.fzRm(key, word);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_RM", e);
        }
    }

    @WriteCommand(Command.FZ_ADD)
    public String fzAdd(String key, String word, Long frequency) {
        try {
            return clusterClient.fzAdd(key, word, frequency);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_ADD", e);
        }
    }

    @WriteCommand(Command.FZ_INCR)
    public Boolean fzIncr(String key, String word, Long incr) {
        try {
            return clusterClient.fzIncr(key, word, incr);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_INCR", e);
        }
    }

    @ReadCommand(Command.FZ_SEARCH)
    public String[] fzSearchAsObj(String key, String query, Integer limit) {
        String val = this.fzSearch(key, query, limit);
        try {
            return objectMapper.readValue(
                    val,
                    new TypeReference<>() {
                    }
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.FZ_SEARCH)
    public String fzSearch(String key, String query, Integer limit) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s:%d", Command.FZ_SEARCH.name(), key, query, limit),
                    () -> this.doFzSearch(key, query, limit)
            );
        } else {
            return this.doFzSearch(key, query, limit);
        }
    }

    private String doFzSearch(String key, String query, Integer limit) {
        try {
            return clusterClient.fzSearch(key, query, limit);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_SEARCH", e);
        }
    }

    @ReadCommand(Command.FZ_SUGGEST)
    public List<FzSuggestModel> fzSuggestAsObj(String key, String query, Integer limit, Integer maxDist) {
        String val = this.fzSuggest(key, query, limit, maxDist);
        try {
            return objectMapper.readValue(
                    val,
                    new TypeReference<>() {
                    }
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.FZ_SUGGEST)
    public String fzSuggest(String key, String query, Integer limit, Integer maxDist) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s:%d:%d", Command.FZ_SUGGEST.name(), key, query, limit, maxDist),
                    () -> this.doFzSuggest(key, query, limit, maxDist)
            );
        } else {
            return this.doFzSuggest(key, query, limit, maxDist);
        }
    }

    private String doFzSuggest(String key, String query, Integer limit, Integer maxDist) {
        try {
            return clusterClient.fzSuggest(key, query, limit, maxDist);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_SUGGEST", e);
        }
    }

    @ReadCommand(Command.FZ_EXACT)
    public FzWordModel fzGetExactAsObj(String key, String word) {
        String val = this.fzGetExact(key, word);
        try {
            return objectMapper.readValue(
                    val,
                    new TypeReference<>() {
                    }
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.FZ_EXACT)
    public String fzGetExact(String key, String word) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.FZ_EXACT.name(), key, word),
                    () -> this.doFzGetExact(key, word)
            );
        } else {
            return this.doFzGetExact(key, word);
        }
    }

    private String doFzGetExact(String key, String word) {
        try {
            return clusterClient.fzGetExact(key, word);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_EXACT", e);
        }
    }

    @ReadCommand(Command.FZ_RANDOM)
    public String[] fzRandomAsObj(String key, Integer limit) {
        String val = this.fzRandom(key, limit);
        try {
            return objectMapper.readValue(
                    val,
                    new TypeReference<>() {
                    }
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.FZ_RANDOM)
    public String fzRandom(String key, Integer limit) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%d", Command.FZ_RANDOM.name(), key, limit),
                    () -> this.doFzRandom(key, limit)
            );
        } else {
            return this.doFzRandom(key, limit);
        }
    }

    private String doFzRandom(String key, Integer limit) {
        try {
            return clusterClient.fzRandom(key, limit);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_RANDOM", e);
        }
    }

    @ReadCommand(Command.FZ_PHONETIC)
    public List<FzWordModel> fzPhoneticAsObj(String key, String input, Integer limit) {
        String val = this.fzPhonetic(key, input, limit);
        try {
            return objectMapper.readValue(
                    val,
                    new TypeReference<>() {
                    }
            );
        } catch (JsonProcessingException e) {
            throw new ParsingException(e);
        }
    }

    @ReadCommand(Command.FZ_PHONETIC)
    public String fzPhonetic(String key, String input, Integer limit) {
        if (this.collapsingHandler != null) {
            return this.collapsingHandler.execute(
                    String.format("%s:%s:%s:%d", Command.FZ_PHONETIC.name(), key, input, limit),
                    () -> this.doFzPhonetic(key, input, limit)
            );
        } else {
            return this.doFzPhonetic(key, input, limit);
        }
    }

    private String doFzPhonetic(String key, String input, Integer limit) {
        try {
            return clusterClient.fzPhonetic(key, input, limit);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_PHONETIC", e);
        }
    }

    @ReadCommand(Command.FZ_EXISTS)
    public Boolean fzExists(String key, String word) {
        if (this.collapsingHandler != null) {
            var result = this.collapsingHandler.execute(
                    String.format("%s:%s:%s", Command.FZ_EXISTS.name(), key, word),
                    () -> String.valueOf(this.doFzExists(key, word))
            );
            return Boolean.valueOf(result);
        } else {
            return this.doFzExists(key, word);
        }
    }

    private Boolean doFzExists(String key, String word) {
        try {
            return clusterClient.fzExists(key, word);
        } catch (Exception e) {
            throw new IntegrationException("ERR_FZ_EXISTS", e);
        }
    }

    public void close() throws Exception {
        try {
            this.clusterClient.close();
        } catch (Exception ignored) {
        }
    }
}