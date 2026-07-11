package org.client.helper;

public final class Constant {
    public static final String DEFAULT = "OK";
    public static final String LEADER = "LEADER";
    public static final String FOLLOWER = "FOLLOWER";
    public static final String PONG = "PONG";

    public static class IResponse {
        public static final String SUCCESS = "1";
        public static final String FAILURE = "0";
    }

    private Constant() {
    }
}
