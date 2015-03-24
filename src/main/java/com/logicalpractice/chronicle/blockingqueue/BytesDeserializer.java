package com.logicalpractice.chronicle.blockingqueue;

import net.openhft.lang.io.Bytes;

/**
 * Allows custom deserialization implementation to be plugged into the ChronicleBlockingQueue.
 */
public interface BytesDeserializer<E> {

    /**
     * Return a new value from the given input.
     *
     * @param bytes required Bytes positioned such that the next byte is the first byte of the serialized value
     * @return deserialized value, never null
     */
    E deserialize(Bytes bytes);
}
