package com.example;


public class FetcherException extends RuntimeException {
    public FetcherException(String message) {
        super(message);
    }

    public FetcherException(String message, Throwable cause) {
        super(message, cause);
    }
}
