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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * BlockingQueue implementation backed by the Chronicle Queue
 */
public class ChronicleBlockingQueue<E> implements BlockingQueue<E>, AutoCloseable {

    private final static int NOT_SET = -1;

    private final Builder<E> config;
    private final BytesSerializer<E> serializer = (E element, Bytes bytes) -> {
        bytes.writeObject(element);
    };
    @SuppressWarnings("unchecked")
    private final BytesDeserializer<E> deserializer = (Bytes bytes) -> (E) bytes.readObject();
    private final ChroniclePosition position;

    private ExcerptAppender cachedAppender;

    private volatile int cachedAppenderSlabIndex = NOT_SET; // is shared between writer and reader
    private ExcerptTailer cachedTailer;

    private int cachedTailerSlabIndex = NOT_SET;

    private int numberOfSlabs;

    private ChronicleBlockingQueue(
            Builder<E> builder
    ) {
        this.config = builder.clone();

        // basic initialisation
        File positionFile = new File(config.storageDirectory, config.name + ".position");
        boolean newPositionFile = !positionFile.exists();

        this.position = new ChroniclePosition(positionFile);
        int slab = lastSlabIndex();
        appender(Math.max(slab, 1)); // force initialisation of the appender, will create the first slab, index = 1

        if (newPositionFile) {
            this.position.slab(firstSlabIndex());
            this.position.index(-1);
        }
        numberOfSlabs = lastSlabIndex() - firstSlabIndex() + 1;
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

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == this)
            throw new IllegalArgumentException();

        if (maxElements <= 0)
            return 0;

        int count;
        for (count = 0; count < maxElements; count ++) {
            E val = this.poll();
            if (val == null) {
                break; // no more elements left
            }
            c.add(val);
        }

        return count;
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

        ExcerptAppender appender;

//        if (shouldRollSlab()) {
//            if (numberOfSlabs >= maxNumberOfSlabs) {
//                we are out of room at the inn
//                return false;
//            }
//            appender = nextAppender();
//        } else {
//        }

        appender = cachedAppender();
        try {
            appender.startExcerpt();
        } catch (IllegalStateException ignored) {
            if (numberOfSlabs < config.maxNumberOfSlabs) {
                appender = nextAppender();
                appender.startExcerpt();
            } else {
                // we're over capacity
                return false;
            }
        }
        serializer.serialise(e, appender);
        appender.finish();
        return true;
    }

    @Override
    public void put(E e) throws InterruptedException {
        Thread currentThread = null;
        // todo either replace spin with lock/notify or improve spin
        while (!offer(e)) {
            if (currentThread == null) {
                currentThread = Thread.currentThread();
            }
            if (currentThread.isInterrupted()) {
                throw new InterruptedException();
            }
        }
    }

    @Override
    public boolean offer(E e, long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        long start = System.nanoTime();
        long deadline = start + NANOSECONDS.convert(timeout, unit);
        Thread currentThread = null;
        // todo either replace spin with lock/notify or improve spin
        while (!offer(e)) {
            if (currentThread == null) {
                currentThread = Thread.currentThread();
            }
            if (currentThread.isInterrupted()) {
                throw new InterruptedException();
            }
            if (System.nanoTime() > deadline) {
                return false;
            }
        }
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        Thread currentThread = null;
        // todo either replace spin with lock/notify or improve spin
        E result;
        while ((result = poll()) == null) {
            if (currentThread == null) {
                currentThread = Thread.currentThread();
            }
            if (currentThread.isInterrupted()) {
                throw new InterruptedException();
            }
        }
        return result;
    }

    @Override
    public E poll(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        long start = System.nanoTime();
        long deadline = start + NANOSECONDS.convert(timeout, unit);
        Thread currentThread = null;
        // todo either replace spin with lock/notify or improve spin
        E result;
        while ((result = poll()) == null) {
            if (currentThread == null) {
                currentThread = Thread.currentThread();
            }
            if (currentThread.isInterrupted()) {
                throw new InterruptedException();
            }
            if (System.nanoTime() > deadline) {
                return null;
            }
        }
        return result;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
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

    private synchronized void deleteSlab(int slab) { // synchronized to force visiblity of change of numberOfSlabs
        File indexFile = new File(config.storageDirectory, slabName(slab) + ".index");
        File dataFile = new File(config.storageDirectory, slabName(slab) + ".data");
        try {
            Files.delete(indexFile.toPath());
            Files.delete(dataFile.toPath());
        } catch(IOException e) {
            throw new RuntimeIOException(e);
        }
        numberOfSlabs -= 1;
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

    private ExcerptAppender nextAppender() {
        numberOfSlabs += 1;
        return appender(nextSlabIndex());
    }

    private ExcerptAppender cachedAppender() {
        return appender(cachedAppenderSlabIndex);
    }

    private ExcerptAppender appender(int slabIndex) {
        if (cachedAppenderSlabIndex != slabIndex) {
            release(cachedAppender);
            cachedAppender = chronicleAppender(slabIndex);
            cachedAppenderSlabIndex = slabIndex;
        }

        return cachedAppender;
    }

    private Chronicle chronicle(int slab) {
        try {
            return ChronicleQueueBuilder
                    .indexed(config.storageDirectory, slabName(slab))
                    .useCheckedExcerpt(true) // self protection
                    .messageCapacity(config.messageCapacity)
                    .dataBlockSize(config.slabBlockSize)
                    .maxDataBlocks(1)
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
        return lastSlabIndex() + 1;
    }

    private int lastSlabIndex() {
        int highest = Arrays.asList(config.storageDirectory.listFiles(this::isSlabIndex))
                .stream()
                .collect(Collectors.summarizingInt(this::slabIndex)).getMax();
        return highest == Integer.MIN_VALUE ? 0 : highest;
    }

    private int firstSlabIndex() {
        return Arrays.asList(config.storageDirectory.listFiles(this::isSlabIndex))
                .stream()
                .collect(Collectors.summarizingInt(this::slabIndex)).getMin();
    }

    private String slabName(int i) {
        return config.name + '-' + i;
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
        if (filename.startsWith(config.name + '-') && firstDotIndex > -1) {
            String indexPart = filename.substring((config.name + '-').length(), firstDotIndex);
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

    public static class Builder<E> implements Cloneable {
        private final File storageDirectory;
        private String name = "chronicleblockingqueue";
        private int maxNumberOfSlabs = Integer.MAX_VALUE;

        // parameters for the underlying chronicle
        private int slabBlockSize = 1024 * 1024 * 64; // 64MB maps to dataBlockSize
        private int messageCapacity = 128 * 1024; // 128KB
        private boolean useCheckedExcerpt = true;

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

        public File storageDirectory() {
            return storageDirectory;
        }

        public Builder<E> name(String name) {
            this.name = Objects.requireNonNull(name);
            return this;
        }

        public String name() {
            return name;
        }

        public Builder<E> maxNumberOfSlabs(int maxSlabs) {
            this.maxNumberOfSlabs = maxSlabs;
            return this;
        }

        public int maxNumberOfSlabs() {
            return maxNumberOfSlabs;
        }

        public int slabBlockSize() {
            return slabBlockSize;
        }

        public Builder<E> slabBlockSize(int slabBlockSize) {
            this.slabBlockSize = slabBlockSize;
            return this;
        }

        public int messageCapacity() {
            return messageCapacity;
        }

        public Builder<E> messageCapacity(int messageCapacity) {
            this.messageCapacity = messageCapacity;
            return this;
        }

        public ChronicleBlockingQueue<E> build() {
            return new ChronicleBlockingQueue<E>(this);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Builder<E> clone() {
            try {
                return (Builder<E>) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
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
