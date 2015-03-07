package com.logicalpractice.chronicle.blockingqueue

import com.google.common.io.Files as GuavaFiles
import spock.lang.AutoCleanup
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

/**
 *
 */
abstract class ChronicleBlockingQueueSpec extends Specification {

  private File tempDir

  File tempDir() {
    if (tempDir == null) {
      tempDir = GuavaFiles.createTempDir()
    }
    tempDir
  }

  ChronicleBlockingQueue standardQueue(HashMap args = [:]) {
    ChronicleBlockingQueue.builder(tempDir())
        .maxPerSlab(args.getOrDefault("maxPerSlab", 10))
        .build()
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
    ChronicleBlockingQueue testObject = ChronicleBlockingQueue.builder(tempDir()).build()

    def "poll returns null if empty"() {
      expect:
      testObject.poll() == null
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

  static class MaxPerSlab extends ChronicleBlockingQueueSpec {
    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue(maxPerSlab: 3)

    def "adding more than maxPerSlab results in additional slab files"() {
      expect: "initial state is three files"
      tempDir().list().length == 3

      when:
      (1..4).each { testObject << it }

      then:
      tempDir().list().length == 5
    }

    def "having added more than maxPerSlab, can still read all the elements in order"() {
      given:
      testObject.addAll(1..15)
      def elements = [], value
      when:
      while ((value = testObject.poll()) != null)
        elements << value

      then:
      elements == (1..15) as List
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

  static class Size extends ChronicleBlockingQueueSpec {
    @AutoCleanup
    ChronicleBlockingQueue testObject = standardQueue()

    def "size is maintained as elements are added and removed"() {
      expect:
      testObject.size() == 0

      when:
      testObject << 1

      then:
      testObject.size() == 1

      when:
      testObject.poll()

      then:
      testObject.size() == 0
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
}
