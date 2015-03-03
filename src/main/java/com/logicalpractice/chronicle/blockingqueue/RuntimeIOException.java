package com.logicalpractice.chronicle.blockingqueue;

import java.io.IOException;

/**
 *
 */
public class RuntimeIOException extends RuntimeException {

    public RuntimeIOException(String message, IOException cause) {
        super(message, cause);
    }

    public RuntimeIOException(IOException cause) {
        super(cause);
    }
}
