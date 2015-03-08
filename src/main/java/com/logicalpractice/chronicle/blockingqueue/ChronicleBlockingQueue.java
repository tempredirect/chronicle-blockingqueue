package com.logicalpractice.chronicle.blockingqueue;

import com.google.common.collect.AbstractIterator;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;
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
    private final BytesSerializer<E> serializer = (E element, Bytes bytes) -> {
        bytes.writeObject(element);
    };
    @SuppressWarnings("unchecked")
    private final BytesDeserializer<E> deserializer = (Bytes bytes) -> (E) bytes.readObject();
    private final int maxPerSlab;
    private final ChroniclePosition position;

    private ExcerptAppender cachedAppender;

    private volatile int cachedAppenderSlabIndex = NOT_SET; // is shared between writer and reader
    private ExcerptTailer cachedTailer;

    private int cachedTailerSlabIndex = NOT_SET;

    private ChronicleBlockingQueue(File storageDirectory, String name, int maxPerSlab) {
        this.storageDirectory = storageDirectory;
        this.name = name;
        this.maxPerSlab = maxPerSlab;

        // basic initialisation
        File positionFile = new File(storageDirectory, name + ".position");
        boolean newPositionFile = !positionFile.exists();

        this.position = new ChroniclePosition(positionFile);
        appender(); // force initialisation of the appender, will create the first slab

        if (newPositionFile) {
            this.position.slab(firstSlabIndex());
            this.position.index(-1);
        }
    }

    public static <E> ChronicleBlockingQueue.Builder<E> builder(File storageDirectory) {
        return new ChronicleBlockingQueue.Builder<E>(storageDirectory);
    }

    @Override
    public int size() {
        int size = 0;
        for (E e : this) {
            size++;
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) return false;

        QueueIterator iter = iterator();
        while (iter.hasNext()) {
            E value = iter.next();
            if (value.equals(o)) {
                iter.close();
                return true;
            }
        }
        return false;
    }

    @NotNull
    @Override
    public QueueIterator iterator() {
        return new QueueIterator(position);
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return asList().toArray();
    }

    @NotNull
    @Override
    public <T> T[] toArray(T[] a) {
        return asList().toArray(a);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object e : c)
            if (!contains(e))
                return false;
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        c.forEach(this::add);
        return true;
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
    public boolean add(E e) {
        if (offer(e))
            return true;
        throw new IllegalStateException();
    }

    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException("null elements are not permitted");
        }
        if (shouldRollSlab()) {
            releaseCachedAppender();
        }
        ExcerptAppender appender = appender();

        appender.startExcerpt();
        serializer.serialise(e, appender);
        appender.finish();
        return true;
    }

    @Override
    public E remove() {
        E value = poll();
        if (value == null) throw new NoSuchElementException();
        return value;
    }

    @Override
    public E poll() {
        int slab = position.slab();
        ExcerptTailer tailer = cachedTailerForSlab(slab);
        toPosition(position, tailer);

        E value = readAndUpdate(tailer, position);
        if (value != null)
            return value;

        // maybe the next slab has some?
        if (slab == cachedAppenderSlabIndex) {
            // ie we're reading the same thing that is being written
            return null; // there is nothing more stop looking
        }

        int nextSlab = position.incrementSlabAndResetIndex();
        tailer = cachedTailerForSlab(nextSlab); // will close the open tailer so we don't have to worry about it
        tailer.toStart();
        deleteSlab(slab);
        return readAndUpdate(tailer, position);
    }

    private void deleteSlab(int slab) {
        File indexFile = new File(storageDirectory, slabName(slab) + ".index");
        File dataFile = new File(storageDirectory, slabName(slab) + ".data");
        try {
            Files.delete(indexFile.toPath());
            Files.delete(dataFile.toPath());
        } catch(IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public E element() {
        E value = peek();
        if (value == null) throw new NoSuchElementException();
        return value;
    }

    @Override
    public E peek() {
        int slab = position.slab();
        ExcerptTailer tailer = cachedTailerForSlab(slab);
        toPosition(position, tailer);

        if (tailer.nextIndex()) {
            return deserializer.deserialise(tailer);
        }

        // maybe the next slab has some?
        if (slab == cachedAppenderSlabIndex) {
            // ie we're reading the same thing that is being written
            return null; // there is nothing more stop looking
        }

        tailer = cachedTailerForSlab(slab + 1);
        tailer.toStart();
        if (tailer.nextIndex()) {
            return deserializer.deserialise(tailer);
        }
        return null;
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

    private void releaseCachedAppender() {
        release(cachedAppender);
        cachedAppender = null;
        cachedTailerSlabIndex = NOT_SET;
    }

    private boolean shouldRollSlab() {
        if (cachedAppender != null) {
            long size = cachedAppender.chronicle().size();
            return size >= maxPerSlab;
        }
        return false;
    }

    private E readAndUpdate(ExcerptTailer tailer, ChroniclePosition position) {
        if (tailer.nextIndex()) {
            E value = deserializer.deserialise(tailer);

            position.index((int) tailer.index());
            return value;
        }
        return null;
    }

    private void toPosition(ChroniclePosition pos, ExcerptTailer tailer) {
        boolean found;
        int index = pos.index();
        if (index == -1) {
            tailer.toStart();
            found = true;
        } else {
            found = tailer.index(pos.index());
        }
        if (!found) {
            throw new IllegalStateException("chronicle position slab:" + pos.slab() + " index:" + index);
        }
    }

    private ExcerptTailer cachedTailerForSlab(int slab) {
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
    public String toString() {
        return "ChronicleBlockingQueue";
    }

    @Override
    public void close() throws Exception {
        release(cachedTailer);
        release(cachedAppender);
        position.close();
    }

    @NotNull
    private ArrayList<E> asList() {
        ArrayList<E> copy = new ArrayList<>();
        for (E value : this) {
            copy.add(value);
        }
        return copy;
    }

    public static class Builder<E> {
        private final File storageDirectory;
        private String name = "chronicleblockingqueue";
        private int maxPerSlab = 1_000_000;

        public Builder(File storageDirectory) {
            if (storageDirectory == null) {
                throw new IllegalArgumentException("storageDirectory is required");
            }
            if (!storageDirectory.isDirectory()) {
                throw new IllegalArgumentException(
                        "storageDirectory :" + storageDirectory
                                + " is not a path to a directory");
            }
            this.storageDirectory = storageDirectory;
        }

        public Builder<E> name(String name) {
            this.name = Objects.requireNonNull(name);
            return this;
        }

        public Builder<E> maxPerSlab(int maxPerSlab) {
            this.maxPerSlab = maxPerSlab;
            return this;
        }

        public ChronicleBlockingQueue<E> build() {
            return new ChronicleBlockingQueue<E>(
                    storageDirectory,
                    name,
                    maxPerSlab);
        }
    }

    public class QueueIterator extends AbstractIterator<E> implements AutoCloseable {
        private ExcerptTailer tailer;
        private int slab;

        QueueIterator(ChroniclePosition position) {
            this.slab = position.slab();
            this.tailer = chronicleTailer(slab);
            toPosition(position, tailer); // navigate to the initial position or die
        }

        @Override
        protected E computeNext() {
            if (tailer.nextIndex()) {
                return deserializer.deserialise(tailer);
            }
            if (slab == cachedAppenderSlabIndex) {
                close();
                return endOfData();
            }
            // move on to the next slab
            slab++;
            release(tailer);
            tailer = chronicleTailer(slab);
            tailer.toStart();

            return computeNext();
        }

        public void close() {
            release(tailer);
            tailer = null; //
        }
    }
}
