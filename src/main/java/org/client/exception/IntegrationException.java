package org.client.exception;

public class IntegrationException extends RuntimeException {
    private final String message;
    private final Exception cause;

    public IntegrationException(String message, Exception cause) {
        this.message = message;
        this.cause = cause;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public Exception getCause() {
        return cause;
    }
}
