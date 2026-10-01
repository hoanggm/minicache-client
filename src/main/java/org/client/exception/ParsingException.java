package org.client.exception;

public class ParsingException extends RuntimeException {
    private final Exception cause;

    public ParsingException(Exception cause) {
        this.cause = cause;
    }

    @Override
    public Exception getCause() {
        return cause;
    }
}
