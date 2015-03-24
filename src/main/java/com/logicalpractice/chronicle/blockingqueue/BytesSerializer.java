package com.logicalpractice.chronicle.blockingqueue;

import net.openhft.lang.io.Bytes;

/**
 * Allows custom serialization implementation to be plugged into the ChronicleBlockingQueue.
 */
public interface BytesSerializer<E> {

    /**
     * Estimate the weight a given element in number of bytes.
     *
     * <p>Maximum number of bytes required to serialise the value into.
     * Note that returning a larger value is ok, a smaller value will cause
     * a runtime exception to be thrown during serialize.
     * </p>
     * <p>Implementors should return -1 if unable to estimate the weight
     * rather than</p>
     * @param element required element
     * @return upper limit of bytes required by serialize or -1 if the weight is unknown
     */
    default int weigh(E element) {
        return -1;
    }

    /**
     * Write the given value to the bytes.
     *
     * @param element required value to be serialized
     * @param bytes required bytes, is expected that the bytes internal position will be advanced
     *              as the implementation writes the value.
     */
    void serialize(E element, Bytes bytes);
}
