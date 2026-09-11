## NUID — a fast unique identifier generator, used for request inboxes.
##
## Derived from `nats-io/nuid` v1.0.1 (Apache-2.0), used by nats.go. See
## PROVENANCE.md.
##
## 22 characters of base62: a 12-character crypto-random prefix plus a
## 10-character sequential part that starts at a pseudo-random value and
## advances by a pseudo-random increment, re-randomising on rollover.
##
## Deviations from Go:
##  - the sequential PRNG is a per-instance `Rand` (Nim's `std/random`) rather
##    than Go's globally seeded `math/rand`. The *contract* is identical
##    (random start, increment in [33,333), re-randomise at rollover); the
##    particular values differ, as they do between any two PRNGs.
##  - the convenience generator is thread-local; connections also own their
##    own generator. Separate connection owners share no mutable NUID state.
##  - `newNuID(seed)` is exposed so tests are deterministic; upstream seeds
##    from crypto entropy only.

import std/[random, times]
import std/sysrand

const
  digits* = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  base* = 62
  preLen* = 12
  seqLen* = 10
  maxSeq* = 839_299_365_868_340_224'i64  ## base^seqLen
  minInc* = 33'i64
  maxInc* = 333'i64
  totalLen* = preLen + seqLen

type
  NuID* = object
    pre*: string
    seq*: int64
    inc*: int64
    r: Rand

proc entropySeed(): int64 =
  var b: array[8, byte]
  if urandom(b):
    var v = 0'i64
    for x in b: v = (v shl 8) or int64(x)
    return v
  int64(epochTime() * 1_000_000.0)

proc randomizePrefix*(n: var NuID) =
  ## New 12-character prefix from crypto entropy.
  var cb: array[preLen, byte]
  if not urandom(cb):
    raise newException(IOError, "nuid: failed generating crypto random number")
  n.pre.setLen(0)
  for i in 0 ..< preLen:
    n.pre.add(digits[int(cb[i]) mod base])

proc resetSequential*(n: var NuID) =
  ## New random start and increment for the sequential part.
  n.seq = n.r.rand(maxSeq - 1)
  n.inc = minInc + n.r.rand(maxInc - minInc - 1)

proc newNuID*(seed = 0'i64): NuID =
  ## `seed == 0` seeds from crypto entropy (upstream behaviour); a non-zero
  ## seed makes the sequential part deterministic.
  result.r = initRand(if seed != 0: seed else: entropySeed())
  result.resetSequential()
  result.randomizePrefix()

proc next*(n: var NuID): string =
  ## Increment and format the next identifier.
  n.seq += n.inc
  if n.seq >= maxSeq:
    n.randomizePrefix()
    n.resetSequential()
  var b: array[totalLen, char]
  for i in 0 ..< preLen: b[i] = n.pre[i]
  var i = totalLen
  var l = n.seq
  while i > preLen:
    dec i
    b[i] = digits[int(l mod base)]
    l = l div base
  result = newString(totalLen)
  for k in 0 ..< totalLen: result[k] = b[k]

var
  globalNuID {.threadvar.}: NuID
  globalReady {.threadvar.}: bool

proc nextId*(): string =
  ## Thread-local convenience generator (upstream `Next()`).
  if not globalReady:
    globalNuID = newNuID()
    globalReady = true
  globalNuID.next()
