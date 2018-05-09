package com.avaya.workflow.impl.edp.lock;

public class GSLockException extends RuntimeException {
    public GSLockException(String message) {
        super(message);
    }

    public GSLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
