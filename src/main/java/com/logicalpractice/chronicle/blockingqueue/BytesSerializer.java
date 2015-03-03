package com.logicalpractice.chronicle.blockingqueue;

import net.openhft.lang.io.Bytes;

/**
 *
 */
public interface BytesSerializer<E> {
    void serialise(E element, Bytes bytes);
}
