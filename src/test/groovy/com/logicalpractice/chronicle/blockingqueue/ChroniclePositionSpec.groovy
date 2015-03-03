package com.logicalpractice.chronicle.blockingqueue

import com.google.common.io.Files
import spock.lang.Specification

/**
 *
 */
class ChroniclePositionSpec extends Specification {

  def tempDir = Files.createTempDir()

  ChroniclePosition testObject = new ChroniclePosition(new File(tempDir, "position.dat"))

  void cleanup() {
    testObject.close()
  }

  def "initial value is zero"() {
    expect:
    testObject.get() == 0L
  }

  def "can set the value and read it"() {
    when:
    testObject.set(22100012312132L)

    then:
    testObject.get() == 22100012312132L
  }

  def "compareAndSwap updates the value"() {
    when:
    def didSwap = testObject.compareAndSwap(0L, 42L)

    then:
    didSwap
    testObject.get() == 42L
  }

  def "compareAndSwap doesn't update the value if not expected"() {
    given:
    testObject.set(1L)

    when:
    def didSwap = testObject.compareAndSwap(0L, 42L)

    then:
    !didSwap
    testObject.get() == 1L
  }
}
