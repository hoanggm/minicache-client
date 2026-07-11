package org.client.helper;

import org.client.pool.MiniCacheClient;

import java.io.IOException;

@FunctionalInterface
public interface ClusterAction {
    String execute(MiniCacheClient client) throws IOException;
}
