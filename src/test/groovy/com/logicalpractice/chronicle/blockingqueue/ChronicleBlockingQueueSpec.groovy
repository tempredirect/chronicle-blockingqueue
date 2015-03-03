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
  }
}
