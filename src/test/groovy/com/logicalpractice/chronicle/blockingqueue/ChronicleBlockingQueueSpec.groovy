package com.logicalpractice.chronicle.blockingqueue

import com.google.common.io.Files
import spock.lang.AutoCleanup
import spock.lang.Specification
import spock.lang.Unroll

/**
 *
 */
abstract class ChronicleBlockingQueueSpec extends Specification {

  private File tempDir

  File tempDir() {
    if (tempDir == null) {
      tempDir = Files.createTempDir()
    }
    tempDir
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
    ChronicleBlockingQueue testObject = ChronicleBlockingQueue.builder(tempDir())
        .maxPerSlab(3)
        .build()

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
}
