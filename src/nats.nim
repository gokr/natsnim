## nats.nim — pure-Nim NATS client (core NATS).
##
## This module is the **frozen shim contract**: it mirrors the subset of
## `natswrapper` (a Futhark FFI binding over `nats.c`) that Niffler's Nim
## sources actually use, so that adopting this library is a `requires`
## change and `sdk/niffler/sdk.nim` does not move. The surface below was
## extracted from the call sites in `sdk/niffler/`, `core/` and `components/`.
##
## Nothing is implemented yet — see ASSESSMENT.md for the phase plan.
## The transport is a single-threaded, poll-driven socket loop: no
## callbacks, no asyncdispatch, no internal threads. `NextMsg(timeout)` is
## the only place the connection blocks.

type
  natsStatus* = cint
    ## Status code, congruent with `nats.c`'s `natsStatus` (NATS_OK == 0).

  natsConnection* = object
    ## Handle to a connection. Fields are private to the implementation.
    discard

  natsSubscription* = object
    ## Handle to a subscription.
    discard

  natsMsg* = object
    ## One received message.
    discard

  NatsConnection* = object
    ## natswrapper-shaped convenience handle (kept for drop-in compatibility).
    conn*: ptr natsConnection

const
  NATS_OK* = 0.natsStatus
  NATS_TIMEOUT* = 21.natsStatus
    ## Sentinel for "no message within the timeout". The numeric value is
    ## ours (nats.c's enum is internal to this library's users); `checkStatus`
    ## and the `== NATS_TIMEOUT` comparisons are the contract.

# --- library lifecycle ------------------------------------------------------

proc nats_Open*(sleepMs: int): natsStatus =
  ## Initialize the client library (`-1` = library default). Retained for
  ## API compatibility: a pure-Nim client has no global C state to set up.
  raise newException(ValueError, "nats_Open: not implemented")

proc nats_Close*() =
  raise newException(ValueError, "nats_Close: not implemented")

# --- connection -------------------------------------------------------------

proc connect*(url: string = "nats://localhost:4222"): NatsConnection =
  ## Connect and complete the INFO/CONNECT handshake.
  raise newException(ValueError, "connect: not implemented")

proc `close`*(nc: var NatsConnection) =
  raise newException(ValueError, "close: not implemented")

proc publish*(nc: NatsConnection, subject: string, data: string) =
  raise newException(ValueError, "publish: not implemented")

proc natsConnection_PublishString*(conn: ptr natsConnection,
                                   subject, data: cstring): natsStatus =
  raise newException(ValueError, "natsConnection_PublishString: not implemented")

proc natsConnection_FlushTimeout*(conn: ptr natsConnection,
                                  timeoutMs: int64): natsStatus =
  raise newException(ValueError, "natsConnection_FlushTimeout: not implemented")

proc natsConnection_GetMaxPayload*(conn: ptr natsConnection): cint =
  raise newException(ValueError, "natsConnection_GetMaxPayload: not implemented")

proc natsConnection_Destroy*(conn: ptr natsConnection) =
  raise newException(ValueError, "natsConnection_Destroy: not implemented")

# --- subscribe / publish-request -------------------------------------------

proc natsConnection_SubscribeSync*(sub: ptr ptr natsSubscription,
                                   conn: ptr natsConnection,
                                   subject: cstring): natsStatus =
  raise newException(ValueError, "natsConnection_SubscribeSync: not implemented")

proc natsConnection_QueueSubscribeSync*(sub: ptr ptr natsSubscription,
                                        conn: ptr natsConnection,
                                        subject, queue: cstring): natsStatus =
  raise newException(ValueError, "natsConnection_QueueSubscribeSync: not implemented")

proc natsConnection_PublishRequest*(conn: ptr natsConnection,
                                    subject, reply, data: cstring,
                                    dataLen: cint): natsStatus =
  raise newException(ValueError, "natsConnection_PublishRequest: not implemented")

proc natsConnection_Request*(msg: ptr ptr natsMsg, conn: ptr natsConnection,
                             subject, data: cstring, dataLen: cint,
                             timeoutMs: int64): natsStatus =
  raise newException(ValueError, "natsConnection_Request: not implemented")

# --- subscription / message -------------------------------------------------

proc natsSubscription_NextMsg*(msg: ptr ptr natsMsg,
                               sub: ptr natsSubscription,
                               timeoutMs: int64): natsStatus =
  raise newException(ValueError, "natsSubscription_NextMsg: not implemented")

proc natsSubscription_Unsubscribe*(sub: ptr natsSubscription): natsStatus =
  raise newException(ValueError, "natsSubscription_Unsubscribe: not implemented")

proc natsSubscription_Destroy*(sub: ptr natsSubscription): natsStatus =
  raise newException(ValueError, "natsSubscription_Destroy: not implemented")

proc natsMsg_GetData*(msg: ptr natsMsg): cstring =
  raise newException(ValueError, "natsMsg_GetData: not implemented")

proc natsMsg_GetDataLength*(msg: ptr natsMsg): cint =
  raise newException(ValueError, "natsMsg_GetDataLength: not implemented")

proc natsMsg_GetSubject*(msg: ptr natsMsg): cstring =
  raise newException(ValueError, "natsMsg_GetSubject: not implemented")

proc natsMsg_GetReply*(msg: ptr natsMsg): cstring =
  raise newException(ValueError, "natsMsg_GetReply: not implemented")

proc natsMsg_Destroy*(msg: ptr natsMsg) =
  raise newException(ValueError, "natsMsg_Destroy: not implemented")

# --- status helpers (implemented in natswrapper today) ----------------------

proc checkStatus*(status: natsStatus): bool {.inline.} =
  status == NATS_OK

proc getErrorString*(status: natsStatus): string =
  raise newException(ValueError, "getErrorString: not implemented")
