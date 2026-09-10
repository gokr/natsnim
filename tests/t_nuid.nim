## NuID tests, ported from nats-io/nuid @ v1.0.1 (Apache-2.0):
## TestDigits, TestGlobalNUIDInit, TestNUIDRollover, TestGUIDLen,
## TestProperPrefix, TestBasicUniqueness.
##
## Deviation: upstream's uniqueness test runs 10,000,000 iterations; this runs
## 200,000 to keep the suite fast. The property under test (no collision) is
## unchanged.

import std/[sets, unittest]
import natsnim/nuid

suite "nuid":

  test "digits length matches base (TestDigits)":
    check digits.len == base

  test "a new generator is initialised (TestGlobalNUIDInit)":
    let n = newNuID()
    check n.pre.len == preLen
    check n.seq != 0

  test "rollover re-randomises the prefix (TestNUIDRollover)":
    var n = newNuID(1)
    n.seq = maxSeq          # force rollover on the next increment
    let oldPre = n.pre
    discard n.next()
    check n.pre != oldPre

  test "length is totalLen (TestGUIDLen)":
    check nextId().len == totalLen
    check totalLen == 22

  test "every character is base62":
    for _ in 0 ..< 100:
      for c in nextId():
        check digits.contains(c)

  test "the prefix is built from the alphabet (TestProperPrefix)":
    for _ in 0 ..< 1_000:
      let n = newNuID()
      check n.pre.len == preLen
      for c in n.pre:
        check digits.contains(c)

  test "the sequential part advances":
    var n = newNuID(7)
    let a = n.next()
    let b = n.next()
    check a != b
    check a.len == b.len

  test "a fixed seed makes the sequential part deterministic":
    var a = newNuID(99)
    var b = newNuID(99)
    # The prefix stays crypto-random (upstream does the same — only the
    # sequential PRNG is seedable), so compare the sequential tails.
    check a.next()[preLen .. ^1] == b.next()[preLen .. ^1]
    check a.next()[preLen .. ^1] == b.next()[preLen .. ^1]
    check a.pre != b.pre

  test "unique across 200k ids (TestBasicUniqueness)":
    var seen = initHashSet[string]()
    for _ in 0 ..< 200_000:
      check not seen.containsOrIncl(nextId())
    check seen.len == 200_000
