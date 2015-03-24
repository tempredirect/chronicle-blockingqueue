package com.logicalpractice.chronicle.blockingqueue

import com.google.common.io.Files as GuavaFiles
import spock.lang.AutoCleanup
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll
import spock.util.concurrent.BlockingVariable

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

abstract class ChronicleBlockingQueueSpec extends Specification {

  private File tempDir

  File tempDir() {
    if (tempDir == null) {
      tempDir = GuavaFiles.createTempDir()
    }
    tempDir
  }

  ChronicleBlockingQueue standardQueue(HashMap args = [:]) {
    def builder = ChronicleBlockingQueue.builder(tempDir())
        .slabBlockSize(args.getOrDefault("slabBlockSize", 8 * 1024))
        .maxNumberOfSlabs(args.getOrDefault("maxNumberOfSlabs", Integer.MAX_VALUE))

    if ('deserializer' in args)
      builder.deserializer(args.deserializer)
    if ('serializer' in args)
      builder.serializer(args.serializer)

    builder.build()
  }

  def cleanup()  {
    if (tempDir != null) {
      Files.walkFileTree(tempDir.toPath(), new SimpleFileVisitor<Path>() {

        @Override
        FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        @Override
        FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
  }
  static class BuilderSpec extends ChronicleBlockingQueueSpec {

    def "builder doesn't allow invalid storage file"() {
      when:
      ChronicleBlockingQueue.builder(new File("/this/directory/should/never/exist"))

      then:
      thrown(IllegalArgumentException)
    }

    def "builder doesn't allow null storage file"() {
      when:
      ChronicleBlockingQueue.builder(null)

      then:
      thrown(IllegalArgumentException)
    }

    def "builder accepts a directory"() {
      expect:
      ChronicleBlockingQueue.builder(tempDir())
    }
  }

  @Unroll
  static class SlabIndex extends ChronicleBlockingQueueSpec {
    ChronicleBlockingQueue testObject = ChronicleBlockingQueue.builder(tempDir()).name("simple").build()

    def "always true"() {
      // this just removes a messy artifact any running the tests in IDEA
      expect:
      true
    }

    def "isSlabIndex(#filename) expect #result"() {
      given:
      def file = new File(filename)
      expect:
      testObject.isSlabIndex(file) == result

      where:
      filename                   || result
      'simple-1.index'           || true
      'simple-01.index'          || true
      'simple-32131231.index'    || true
      'simple-01.data'           || false
      'simple--1.data'           || false
      'notsimple-32131231.index' || false
      'simple-dsas.index'        || false
      'simple-.index'            || false
    }

    def "slabIndex(#filename) expect #result"() {
      given:
      def file = new File(filename)
      expect:
      testObject.slabIndex(file) == result

      where:
      filename                   || result
      'simple-1.index'           || 1
      'simple-01.index'          || 1
      'simple-32131231.index'    || 32131231
    }
  }

  static class RemoveOperations extends ChronicleBlockingQueueSpec {
    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue()

    def "poll returns null if empty"() {
      expect:
      testObject.poll() == null
    }

    def "poll returns null if empty and called many times"() {
      expect:
      (1..10).every { testObject.poll() == null }
    }

    def "remove throws on empty"() {
      when:
      testObject.remove()

      then:
      thrown NoSuchElementException
    }

    def "poll returns the elements in the order they where appended"() {
      given:
      def input = [1, 2, 3, 4, 5]
      def elements = []
      def value

      testObject.addAll(input)

      when: "read all the elements in the queue"
      while ((value = testObject.poll()) != null)
        elements << value

      then:
      elements == input
    }

    def "removing returns the elements in the order they where appended"() {
      given:
      def input = 1..5 as List
      def elements = []

      testObject.addAll(input)

      when: "read all the elements in the queue"
      5.times { elements << testObject.remove() }

      then:
      elements == input
    }
  }

  static class AppendToQueue extends ChronicleBlockingQueueSpec {

    @AutoCleanup
    ChronicleBlockingQueue testObject = ChronicleBlockingQueue.builder(tempDir()).build()

    def "can add simple items to the queue"() {
      expect:
      testObject.add(1)
    }

    def "offer simple item to the queue"() {
      expect:
      testObject.offer(1)
    }

    def "adding lets poll return the value"() {
      when:
      testObject.add(1)

      then:
      testObject.poll() == 1
    }

    def "can offer many elements"() {
      expect:
      (1..10).every { testObject.offer(it) }
    }

    def "add(null) throws NPE"() {
      when:
      testObject.add(null)

      then:
      thrown NullPointerException
    }
  }

  @Timeout(value = 5, unit = SECONDS)
  static class SlabFileManagement extends ChronicleBlockingQueueSpec {
    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue(maxNumberOfSlabs:3)

    def "adding more than maxPerSlab results in additional slab files"() {
      expect: "initial state is three files"
      tempDir().list().length == 3

      when: 'fill the Q'
      while (testObject.offer("a message"));

      then:
      tempDir().list().length == 2 * 3 + 1
    }

    def "removing elements should clean up defunct slabs"() {
      given: 'a full Q'
      while (testObject.offer("a message"));
      def size = testObject.size()

      expect:
      tempDir().list().length == 2 * 3 + 1 // 2 files per slab, plus position file

      when: 'remove half the elements'
      (size / 2).times { testObject.remove() }

      then: 'one pair of .data and .index files must be removed'
      tempDir().list().length == 2 * 2 + 1 // one less pair of files

      when: 'empty the remaining elements'
      while ( testObject.poll() ) ;

      then: 'only the live and position files remain'
      tempDir().list().length == 2 + 1
    }
  }

  static class Iteration extends ChronicleBlockingQueueSpec {

    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue()

    def "over the elements of the queue"() {
      given:
      testObject.addAll(1..15)

      expect:
      testObject.iterator().collect {it} == 1..15 as List
    }

    def "iterates from the tail"() {
      given:
      testObject.addAll(1..10)

      when:
      3.times { testObject.poll() }

      then:
      testObject.iterator().collect {it} == 4..10 as List
    }

    def "iterator.remove() is not supported"() {
      given:
      testObject.add(1)
      def iterator = testObject.iterator()
      iterator.next()

      when:
      iterator.remove()

      then:
      thrown UnsupportedOperationException
    }

    def "iterator is empty on empty queue"() {
      expect:
      ! testObject.iterator().hasNext()

      when:
      testObject.addAll(1..15)
      while(testObject.poll()); // drain it

      then:
      ! testObject.iterator().hasNext()
    }
  }

  static class Peek extends ChronicleBlockingQueueSpec {
    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue()

    def "peek return null on empty queue"() {
      expect:
      testObject.peek() == null
    }

    def "element throws NoSuchElementException on empty queue"() {
      when:
      testObject.element()

      then:
      thrown NoSuchElementException
    }

    def "peek returns the current tail"() {
      given:
      testObject.addAll(1..5)

      expect:
      testObject.peek() == 1
    }

    def "element returns the current tail"() {
      given:
      testObject.addAll(1..3)

      expect:
      testObject.element() == 1
    }
  }

  static class Size extends ChronicleBlockingQueueSpec {
    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue()

    def "size is maintained as elements are added and removed"() {
      expect:
      testObject.size() == 0

      when:
      testObject.add(1)

      then:
      testObject.size() == 1

      when:
      testObject.poll()

      then:
      testObject.size() == 0
    }
  }

  @Timeout(value = 5, unit = SECONDS)
  static class Capacity extends ChronicleBlockingQueueSpec {

    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue(slabBlockSize: 1024, maxNumberOfSlabs: 3)

    def "offer stops accepting entries when full"() {
      given: "a full Q"
      while (testObject.offer(1));

      expect:
      ! testObject.offer(666)
    }

    def "add must throw when full"() {
      given: "a full Q"
      while (testObject.offer(1));

      when:
      testObject.add(666)

      then:
      thrown IllegalStateException
    }

    def "offer must begin accepting once a slab worths has been removed"() {
      given: "a full Q"
      while (testObject.offer(1));
      def size = testObject.size()

      when:
      (size / 2).times { testObject.remove() }

      then:
      testObject.offer(666)
    }
  }

  static class Contains extends ChronicleBlockingQueueSpec {

    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue()

    def "contains(thing)"() {
      given:
      testObject.addAll(1..15)

      expect:
      testObject.contains(5)

      and:
      !testObject.contains(20)
    }

    def "containsAll(things)"() {
      given:
      testObject.addAll(1..15)

      expect:
      testObject.containsAll(1..3)
      testObject.containsAll(7..13)

      and:
      ! testObject.containsAll(10..20)
    }
  }

  static class ToArray extends ChronicleBlockingQueueSpec {
    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue()

    def "toArray()"() {
      given:
      testObject.addAll(1..15)

      expect:
      testObject.toArray() as List == 1..15 as List
    }

    def "toArray(E[0])"() {
      given:
      testObject.addAll(1..15)
      def intArray = new Integer[0]

      when:
      def result = testObject.toArray(intArray)

      then:
      result as List == 1..15 as List
      result.class.array
      result.class.componentType == Integer
    }

    def "toArray(E[size()])"() {
      given:
      testObject.addAll(1..15)
      def intArray = new Integer[testObject.size()]

      when:
      def result = testObject.toArray(intArray)

      then:
      result.is(intArray)
    }
  }

  @Timeout(value = 5, unit = SECONDS)
  public static class BlockingOperations extends ChronicleBlockingQueueSpec {

    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue(maxNumberOfSlabs: 3)

    @AutoCleanup("shutdownNow")
    ExecutorService executor = Executors.newCachedThreadPool()

    def "put blocks until there we remove enough space"() {
      given: 'a full Q'
      while (testObject.offer(1));
      def size = testObject.size()
      def putStarted = new BlockingVariable()

      when:
      def put = executor.submit({
        putStarted.set(true)
        testObject.put(2)
      }, Boolean.TRUE)

      then: 'put has started but not finished'
      putStarted.get()
      ! put.isDone()

      when: 'empty half the Q'
      (size/2).times { testObject.remove() }

      then: 'put should complete'
      put.get()

      and: 'the Q should contain the value 2'
      testObject.any { it == 2 }

      cleanup:
      put?.cancel(true)
    }

    def "take blocks until element is available"() {
      given:
      def taking = new BlockingVariable()
      def taker = executor.submit({
        taking.set(true)
        testObject.take()
      } as Callable)

      when:
      taking.get()
      testObject << 42

      then:
      taker.get() == 42
    }

    def "put is interruptable"() {
      given: 'a full Q'
      def caught = null
      while (testObject.offer(1));
      Thread put = Thread.start {
        try {
          testObject.put(1)
        } catch (InterruptedException e) {
          caught = e
        }
      }

      when:
      put.interrupt()
      put.join()

      then:
      caught instanceof InterruptedException

      cleanup:
      put?.stop()
    }

    def "take is interruptable"() {
      given:
      def caught = null
      def taking = new BlockingVariable()
      Thread take = Thread.start {
        taking.set(true)
        try {
          testObject.take()
        } catch (Exception e) {
          caught = e
        }
      }

      expect:
      taking.get() == true

      when:
      Thread.sleep(1)
      take.interrupt()
      take.join()

      then:
      caught instanceof InterruptedException

      cleanup:
      take?.stop()
    }

    def "offer with timeout - times out in ~timeout"() {
      given: 'a full Q'
      while (testObject.offer(1));

      def start = System.nanoTime()

      when:
      def result = testObject.offer(2, 1, MILLISECONDS)
      def finished = System.nanoTime()

      then:
      !result
      def time = finished - start
      time >= MILLISECONDS.toNanos(1)
      time <= MILLISECONDS.toNanos(10) // high upper limit
    }

    def "poll with timeout - times out in ~timeout"() {
      given:
      def start = System.nanoTime()

      when:
      def result = testObject.poll(1, MILLISECONDS)
      def finished = System.nanoTime()

      then:
      result == null
      def time = (finished - start)
      time >= MILLISECONDS.toNanos(1)
      time <= MILLISECONDS.toNanos(10)
    }
  }

  static class Drain extends ChronicleBlockingQueueSpec {

    def testObject = standardQueue()


    def "drain(self) -throws illegal argument exception"() {
      when:
      testObject.drainTo(testObject)

      then:
      thrown IllegalArgumentException
    }

    def "drain(collect) drains all the elements to the given collection"() {
      given:
      testObject.addAll(1..15)
      def target = []

      when:
      def transferred = testObject.drainTo target

      then:
      testObject.empty
      transferred == 15
      target == (1..15) as List
    }

    def "drain(self, int) -throws illegal argument exception"() {
      when:
      testObject.drainTo(testObject, 42)

      then:
      thrown IllegalArgumentException
    }

    def "drain(collect, max) drains max elements to the given collection"() {
      given:
      testObject.addAll(1..15)
      def target = []

      when:
      def transferred = testObject.drainTo(target, 10)

      then:
      testObject.size() == 5
      transferred == 10
      target.size() == 10
      target == (1..10) as List
    }
  }

  static class Serialisation extends ChronicleBlockingQueueSpec {

    def "custom serializer/deserializer pair"() {
      given:
      def testObject = standardQueue(
          serializer: { val, bytes -> bytes.writeInt(val) },
          deserializer: { bytes -> bytes.readInt() }
      )

      when:
      testObject.addAll(1..50)
      def output = []
      testObject.drainTo output

      then:
      output == (1..50) as List
    }

    def "custom serializer but not deserializer explodes"() {
      given:
      def testObject = standardQueue(
          serializer: { val, bytes -> bytes.writeInt(val) }
      )
      testObject.addAll(1..50)

      when:
      testObject.poll()

      then:
      thrown IllegalStateException
    }
  }
}
