## Subject and queue-name validation.
##
## Derived from `nats-io/nats.go` `nats.go` @ 1ffb90b (`badSubject`,
## `badQueue`). See PROVENANCE.md.
##
## Note: the *client* never does wildcard matching — the server routes by
## subscription id, and `MSG` frames carry the sid. This module is only about
## rejecting names that cannot be published or subscribed to.

proc badSubject*(subj: string): bool =
  ## True when `subj` is unusable: empty, contains whitespace, or has an empty
  ## token (empty string, leading/trailing '.', or 'a..b'). Mirrors upstream's
  ## whitespace check plus its "no empty token" split check.
  var tokenLen = 0
  for c in subj:
    case c
    of ' ', '\t', '\r', '\n': return true
    of '.':
      if tokenLen == 0: return true
      tokenLen = 0
    else:
      inc tokenLen
  tokenLen == 0

proc validSubject*(subj: string): bool =
  not badSubject(subj)

proc badQueue*(qname: string): bool =
  ## True when a queue-group name contains whitespace (upstream's only rule).
  for c in qname:
    case c
    of ' ', '\t', '\r', '\n': return true
    else: discard
  false
