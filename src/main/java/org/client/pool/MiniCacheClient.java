package org.client.pool;

import jdk.net.ExtendedSocketOptions;
import org.client.helper.Constant;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class MiniCacheClient implements AutoCloseable {
    private static final byte MAGIC_BYTE = 0x4D;
    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private boolean isConnected = false;
    private final MiniCachePool pool;
    private final int clientConnectTimeout;
    private final int clientReadTimeout;
    private final int bufferSize;

    public MiniCacheClient(String host, int port,
                           int clientConnectTimeout, int clientReadTimeout,
                           int bufferSize, MiniCachePool pool) {
        this.host = host;
        this.port = port;
        this.pool = pool;
        this.clientConnectTimeout = clientConnectTimeout;
        this.clientReadTimeout = clientReadTimeout;
        this.bufferSize = bufferSize;
    }

    public void connect() throws IOException {
        if (isConnected) return;
        this.socket = new Socket();
        this.socket.connect(new InetSocketAddress(host, port), this.clientConnectTimeout);
        this.socket.setSoTimeout(this.clientReadTimeout);
        this.socket.setTcpNoDelay(true);
        this.socket.setKeepAlive(true);
        try {
            this.socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, 60);
            this.socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, 10);
            this.socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, 10);
        } catch (UnsupportedOperationException ignored) {
        }
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), bufferSize));
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), bufferSize));
        this.isConnected = true;
    }

    public String set(String key, String value) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x03, key, value, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String set(String key, String value, Boolean notExist) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x03, key, value, notExist ? "1" : "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String set(String key, String value, Boolean notExist, Integer timeToLive) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x03, key, value, notExist ? "1" : "0", timeToLive,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String get(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x02, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public boolean exists(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x06, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public boolean del(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x04, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public String ping() throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x01, null, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String bfInit(String key, Integer expectedElements, Double falsePositive) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x08, key, null, "0", 0,
                expectedElements, falsePositive, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String bfAdd(String key, String value) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x09, key, value, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public boolean bfExist(String key, String value) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x10, key, value, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public boolean bfRm(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x11, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public boolean bfRs(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x12, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public String zScore(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x21, key, null, "0", 0,
                null, null, member, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String zAdd(String key, Double score, String member, String value) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x13, key, value, "0", 0,
                null, null, member, null,
                null, null, score, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public boolean zRem(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x19, key, null, "0", 0,
                null, null, member, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public boolean zDel(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x20, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public String zRank(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x16, key, null, "0", 0,
                null, null, member, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String zRangeByPositions(String key, Integer start, Integer stop) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x15, key, null, "0", 0,
                null, null, null, null,
                start, stop, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String zGetByPosition(String key, Integer position) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x14, key, null, "0", 0,
                null, null, null, position,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public boolean zIncrBy(String key, Double increment, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x17, key, null, "0", 0,
                null, null, member, null,
                null, null, increment, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public String zRangeByScore(String key, Double minScore, Double maxScore) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x18, key, null, "0", 0,
                null, null, null, null,
                null, null, null, minScore, maxScore,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String zTop(String key, Integer top) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x29, key, null, "0", 0,
                null, null, null, null,
                top, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String geoAdd(String key, String member, Double lat, Double lon) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x22, key, null, "0", 0,
                null, null, member, null,
                null, null, null, null, null,
                member, lat, lon, null, null, null, null);
        return handleServerResponse();
    }

    public String geoSearch(String key, Double centerLat, Double centerLon, Double radiusMeters, Integer limit) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x23, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, centerLat, centerLon, null, radiusMeters, limit, null);
        return handleServerResponse();
    }

    public String geoDist(String key, String member1, String member2) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x24, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                member1, null, null, member2, null, null, null);
        return handleServerResponse();
    }

    public boolean geoDel(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x25, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public boolean geoRm(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x26, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                member, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public String geoGet(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x27, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                member, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String geoNb(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x28, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                member, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public boolean geoExists(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x30, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                member, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public String geoEncode(String key, String member) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x31, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                member, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public String hSet(String key, String field, String value) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x32, key, value, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                field);
        return handleServerResponse();
    }

    public String hGet(String key, String field) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x33, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                field);
        return handleServerResponse();
    }

    public String hGetAll(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x34, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return handleServerResponse();
    }

    public boolean hRm(String key, String field) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x35, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                field);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    public boolean hDel(String key) throws IOException {
        ensureConnected();
        sendBinaryRequest((byte) 0x36, key, null, "0", 0,
                null, null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null,
                null);
        return Constant.IResponse.SUCCESS.equalsIgnoreCase(handleServerResponse());
    }

    private void ensureConnected() throws IOException {
        if (!isConnected || socket == null || socket.isClosed()) {
            connect();
        }
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.returnResource(this);
        } else {
            destroy();
        }
    }

    public void destroy() {
        try {
            if (isConnected) {
                sendBinaryRequest((byte) 0x00, null, null, "0", 0,
                        null, null, null, null,
                        null, null, null, null, null,
                        null, null, null, null, null, null,
                        null);
            }
        } catch (Exception ignored) {
        }
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException ignored) {
        }
        this.isConnected = false;
    }

    private void sendBinaryRequest(byte opcode, String key, String value, String notExists,
                                   Integer timeToLive, Integer bloomFilterExpectedElements,
                                   Double bloomFilterFalsePositiveRate, String zsMember,
                                   Integer zsIdx, Integer zsStartIdx, Integer zsStopIdx,
                                   Double zsScore, Double zsStartScr, Double zsStopScr,
                                   String geoMem, Double geoLat, Double geoLon,
                                   String geoMem2, Double geoRadius, Integer limit,
                                   String hsField) throws IOException {
        try {
            byte[] keyBytes = (key != null) ? key.getBytes(StandardCharsets.UTF_8) : new byte[0];
            byte[] valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];

            out.writeByte(MAGIC_BYTE);
            out.writeByte(opcode);
            out.writeShort(keyBytes.length);
            out.writeInt(valueBytes.length);

            if (keyBytes.length > 0) out.write(keyBytes);
            if (valueBytes.length > 0) out.write(valueBytes);

            short sNotExists = (notExists != null) ? Short.parseShort(notExists) : 0;
            int iTtl = (timeToLive != null) ? timeToLive : 0;
            out.writeShort(sNotExists);
            out.writeInt(iTtl);

            // for bloom-filter
            out.writeInt(bloomFilterExpectedElements != null
                    ? bloomFilterExpectedElements
                    : 0);
            out.writeDouble(bloomFilterFalsePositiveRate != null
                    ? bloomFilterFalsePositiveRate
                    : 0D);

            // for skip-list
            out.writeDouble(zsScore != null ? zsScore : 0d);
            out.writeDouble(zsStartScr != null ? zsStartScr : 0d);
            out.writeDouble(zsStopScr != null ? zsStopScr : 0d);
            out.writeInt(zsIdx != null ? zsIdx : 0);
            out.writeInt(zsStartIdx != null ? zsStartIdx : 0);
            out.writeInt(zsStopIdx != null ? zsStopIdx : 0);
            out.writeUTF(zsMember != null ? zsMember : "");

            //for geo-hash
            out.writeUTF(geoMem != null ? geoMem : "");
            out.writeDouble(geoLat != null ? geoLat : 0d);
            out.writeDouble(geoLon != null ? geoLon : 0d);
            out.writeUTF(geoMem2 != null ? geoMem2 : "");
            out.writeDouble(geoRadius != null ? geoRadius : 0d);

            out.writeInt(limit != null ? limit : 0);
            out.writeUTF(hsField != null ? hsField : "");

            out.flush();
        } catch (IOException ex) {
            this.isConnected = false;
            throw ex;
        }
    }

    private String handleServerResponse() throws IOException {
        byte magic = in.readByte();
        if (magic != MAGIC_BYTE) {
            throw new IOException("Protocol corruption");
        }
        byte status = in.readByte();
        int dataLength = in.readInt();

        byte[] dataBytes = new byte[dataLength];
        if (dataLength > 0) in.readFully(dataBytes);
        String resultString = new String(dataBytes, StandardCharsets.UTF_8);

        return switch (status) {
            case 0x00 -> {
                switch (resultString) {
                    case Constant.PONG -> {
                        yield Constant.PONG;
                    }
                    case Constant.LEADER -> {
                        yield Constant.LEADER;
                    }
                    case Constant.FOLLOWER -> {
                        yield Constant.FOLLOWER;
                    }
                    default -> {
                        yield Constant.DEFAULT;
                    }
                }
            }
            case 0x01, 0x03 -> resultString;
            case 0x02 -> null;
            case (byte) 0xFF -> throw new RuntimeException("Server Error: " + resultString);
            default -> throw new IOException("Unknown status: " + status);
        };
    }
}