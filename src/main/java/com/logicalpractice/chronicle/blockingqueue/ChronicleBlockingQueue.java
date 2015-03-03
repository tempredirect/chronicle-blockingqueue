package com.logicalpractice.chronicle.blockingqueue;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * BlockingQueue implementation backed by the Chronicle Queue
 */
public class ChronicleBlockingQueue<E> implements Queue<E>, AutoCloseable {

    private final static int NOT_SET = -1;

    private final File storageDirectory;
    private final String name;
    private final BytesSerializer<E> serializer = (E element, Bytes bytes) -> { bytes.writeObject(element);};
    @SuppressWarnings("unchecked")
    private final BytesDeserializer<E> deserializer = (Bytes bytes) -> (E)bytes.readObject();

    private ExcerptAppender cachedAppender;
    private volatile int cachedAppenderSlabIndex = NOT_SET; // is shared between writer and reader

    private ExcerptTailer cachedTailer;
    private int cachedTailerSlabIndex = NOT_SET;

    private ChroniclePosition position;

    private ChronicleBlockingQueue(File storageDirectory, String name) {
        this.storageDirectory = storageDirectory;
        this.name = name;
        this.position = new ChroniclePosition(new File(storageDirectory, name + ".position"));
        appender(); // force initialisation of the appender, will create the first slab
    }

    public static <E> ChronicleBlockingQueue.Builder<E> builder(File storageDirectory) {
        return new ChronicleBlockingQueue.Builder<E>(storageDirectory);
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @NotNull
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(E e) {
        if (offer(e))
            return true;
        throw new IllegalStateException();
    }

    private ExcerptAppender appender() {
        if (cachedAppender == null) { // todo add slab capacity check
            int slab = nextSlabIndex();
            cachedAppender = chronicleAppender(slab);
            cachedAppenderSlabIndex = slab;
        }

        return cachedAppender;
    }

    private Chronicle chronicle(int slab) {
        try {
            return ChronicleQueueBuilder
                    .indexed(storageDirectory, slabName(slab))
                    .build();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private ExcerptAppender chronicleAppender(int slab) {
        try {
            return chronicle(slab).createAppender();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private ExcerptTailer chronicleTailer(int slab) {
        try {
            return chronicle(slab).createTailer();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private int nextSlabIndex() {
        int highest = Arrays.asList(storageDirectory.listFiles(this::isSlabIndex))
                .stream()
                .collect(Collectors.summarizingInt(this::slabIndex)).getMax();
        return highest == Integer.MIN_VALUE ? 1 : highest + 1;
    }

    private int firstSlabIndex() {
        return Arrays.asList(storageDirectory.listFiles(this::isSlabIndex))
                .stream()
                .collect(Collectors.summarizingInt(this::slabIndex)).getMin();
    }


    private String slabName(int i) {
        return name + '-' + i;
    }

    boolean isSlabIndex(File file) {
        return slabIndex(file) > -1;
    }

    private boolean numeric(String indexPart) {
        int len = indexPart.length();
        if (len == 0) return false;
        for (int i = 0; i < len; i++) {
            if (!Character.isDigit(indexPart.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private int slabIndex(File file) {
        String filename = file.getName();
        int firstDotIndex = filename.indexOf('.');
        if (filename.startsWith(name + '-') && firstDotIndex > -1) {
            String indexPart = filename.substring((name + '-').length(), firstDotIndex);
            String extension = filename.substring(firstDotIndex);
            if (numeric(indexPart) && extension.equals(".index")) {
                return Integer.parseInt(indexPart, 10);
            }
        }
        return -1;
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(E e) {
        ExcerptAppender appender = appender();
        appender.startExcerpt();
        serializer.serialise(e, appender);
        appender.finish();
        return true;
    }

    @Override
    public E remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E poll() {
        ExcerptTailer tailer = tailerAtPosition(position);

        E value = readAndUpdate(tailer, position);
        if (value != null)
            return value;

        // maybe the next slab has some?
        if (cachedTailerSlabIndex == cachedAppenderSlabIndex) {
            // ie we're reading the same thing that is being written
            return null; // there is nothing more stop looking
        }

        int slab = position.incrementSlabAndResetIndex();
        tailer = tailerForSlab(slab);
        return readAndUpdate(tailer, position);
    }

    private E readAndUpdate(ExcerptTailer tailer, ChroniclePosition position) {
        if (tailer.nextIndex()) {
            E value = deserializer.deserialise(tailer);

            if (position != null)
                position.index((int)tailer.index());
            return value;
        }
        return null;
    }

    private ExcerptTailer tailerAtPosition(ChroniclePosition pos) {
        int slab = pos.slab();

        if (slab == 0){
            slab = firstSlabIndex();
        }
        ExcerptTailer tailer = tailerForSlab(slab);

        boolean found;
        int index = pos.index();
        if (index == 0) {
            tailer.toStart();
            found = true;
        } else {
            found = tailer.index(pos.index());
        }
        if (!found) {
            throw new IllegalStateException("chronicle position slab:" + slab + " index:" + index);
        }
        return tailer;
    }

    private ExcerptTailer tailerForSlab(int slab) {
        ExcerptTailer tailer;
        if (slab != cachedTailerSlabIndex) {
            release(cachedTailer);
            tailer = cachedTailer = chronicleTailer(slab);
            cachedTailerSlabIndex = slab;
        } else {
            tailer = cachedTailer;
        }
        return tailer;
    }

    private void release(ExcerptTailer tailer) {
        if (tailer != null) {
            try {
                tailer.chronicle().close();
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    private void release(ExcerptAppender appender) {
        if (appender != null) {
            try {
                appender.chronicle().close();
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    @Override
    public E element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "ChronicleBlockingQueue";
    }

    @Override
    public void close() throws Exception {
        release(cachedTailer);
        release(cachedAppender);
        position.close();
    }

    public static class Builder<E> {
        private final File storageDirectory;
        private String name = "chronicleblockingqueue";

        public Builder(File storageDirectory) {
            if (storageDirectory == null) {
                throw new IllegalArgumentException("storageDirectory is required");
            }
            if (!storageDirectory.isDirectory()) {
                throw new IllegalArgumentException("storageDirectory :" + storageDirectory
                        + " is not a path to a directory");
            }
            this.storageDirectory = storageDirectory;
        }

        public Builder<E> name(String name) {
            this.name = Objects.requireNonNull(name);
            return this;
        }

        public ChronicleBlockingQueue<E> build() {
            return new ChronicleBlockingQueue<E>(
                    storageDirectory,
                    name
            );
        }
    }
}
