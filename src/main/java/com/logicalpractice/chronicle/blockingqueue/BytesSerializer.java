package com.logicalpractice.chronicle.blockingqueue;

import net.openhft.lang.io.Bytes;

/**
 * Allows custom serialization implementation to be plugged into the ChronicleBlockingQueue.
 */
public interface BytesSerializer<E> {

    /**
     * Write the given value to the bytes.
     *
     * @param element required value to be serialized
     * @param bytes required bytes, is expected that the bytes internal position will be advanced
     *              as the implementation writes the value.
     */
    void serialize(E element, Bytes bytes);
}
