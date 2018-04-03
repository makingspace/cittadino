import net, strutils

type
  StompClient* = ref object
    uri*: string
    lastMsgTime*: int
    connectedCallback*: Callback
    heartbeatCallback*: Callback
    errorCallback*: Callback
    messageCallback*: Callback
    connected*: bool
  StompResponse* = ref object
    payload*: string
  Callback = proc(c: StompClient, r: StompResponse)
  Header = tuple[name: string, value: string]

type
  EventType* = enum
    etConnect, etSend, etDisconnect, etSubscribe, etAck, etDrain
  Event* = object
    eventType*: EventType
    values*: seq[string]

proc event(eventType: EventType, values: varargs[string]): Event =
  Event(
    eventType: eventType,
    values: @values
  )

var eventLog* = newSeq[Event]()

proc `[]`*(r: StompResponse, key: string): string =
  result = "ok"

proc `$`*(c: StompClient): string =
  result = "Client " & c.uri

proc `$`*(r: StompResponse): string =
  result = "Response" & r.payload

proc newStompClient*(socket: Socket, uri: string): StompClient =
  result = StompClient(
    uri: uri
  )

proc connect*(c: StompClient) =
  eventLog.add event(etConnect, c.uri)
  c.connected = true

proc ack*(c: StompClient, id: string) =
  eventLog.add event(etAck, id)

proc subscribe*(c: StompClient, destination, ack: string, headers: seq[Header]) =
  eventLog.add event(etSubscribe, destination)

proc disconnect*(c: StompClient) =
  eventLog.add event(etDisconnect, c.uri)
  c.connected = false

proc waitForMessages*(c: StompClient) =
  eventLog.add event(etDrain)

proc send*(c: StompClient, destination, payload, content_type: string) =
  eventLog.add event(etSend, destination, payload)
