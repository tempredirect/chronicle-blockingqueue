package com.logicalpractice.chronicle.blockingqueue;

import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.io.VanillaMappedFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public class ChroniclePosition implements Closeable {

    private final VanillaMappedBytes bytes;

    public ChroniclePosition(File position) {
        try {
            bytes = VanillaMappedFile.readWriteBytes(position, 8);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public long get() {
        return bytes.readVolatileLong(0L);
    }

    public boolean compareAndSwap(long expect, long update) {
        return bytes.compareAndSwapLong(0L, expect, update);
    }

    public void set(long update) {
        bytes.writeOrderedLong(0L, update);
    }

    public void close() {
        bytes.close();
    }

    public int slab() {
        // hi 32 bits
        return (int) ((get() >> 32));
    }

    public void slab(int newSlab) {
        long value = get() & 0xffffffffL; // clean the upper 32bits
        set(value | ((long)newSlab) << 32);
    }

    public int incrementSlabAndResetIndex() {
        long slab = slab() + 1;
        set(slab << 32); // sets the index to zero in the process
        return (int)slab;
    }

    public int index() {
        // lower 32 bits
        return (int) get();
    }

    public void index(int newIndex) {
        long value = get() & 0xffffffff_00000000L; // get and clean lower 32 bits
        long longNewIndex = ((long)newIndex) & 0xffffffffL; // else if newIndex is negative the upcase will populate
                                                            // the higher 32 bits of the long
        set( value | longNewIndex );
    }
}
