package org.client.collapser;

import org.client.exception.IntegrationException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class SingleFlightCollapsingHandler {
    private final ConcurrentHashMap<String, CompletableFuture<String>> inflight = new ConcurrentHashMap<>();

    private final long timeoutMillis;

    public SingleFlightCollapsingHandler() {
        this(3000);
    }

    public SingleFlightCollapsingHandler(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Thực thi request có áp dụng Request Collapsing.
     *
     * @param collapseKey Key đại diện cho request
     * @param supplier    Hàm call thực tế
     */
    public String execute(String collapseKey, Supplier<String> supplier) {
        CompletableFuture<String> future = inflight.computeIfAbsent(collapseKey, k -> {
            CompletableFuture<String> newFuture = new CompletableFuture<>();

            CompletableFuture.supplyAsync(supplier)
                    .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                    .whenComplete((result, throwable) -> {
                        inflight.remove(k);

                        if (throwable != null) {
                            newFuture.completeExceptionally(throwable);
                        } else {
                            newFuture.complete(result);
                        }
                    });

            return newFuture;
        });

        try {
            return future.join();
        } catch (Exception ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof IntegrationException) {
                throw (IntegrationException) cause;
            }
            throw new IntegrationException("ERR_SINGLE_FLIGHT_TIMEOUT", ex);
        }
    }
}
