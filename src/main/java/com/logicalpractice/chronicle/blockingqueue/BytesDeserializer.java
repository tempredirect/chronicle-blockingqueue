package com.logicalpractice.chronicle.blockingqueue;

import net.openhft.lang.io.Bytes;

/**
 *
 */
public interface BytesDeserializer<E> {
    E deserialise(Bytes bytes);
}
