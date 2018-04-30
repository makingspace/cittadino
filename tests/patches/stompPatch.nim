import net, strutils

const
  testUrl* = "stomp://localhost/"
  testModelExchangeName* = "model_exchange"
  testNameSpace* = "testing"
  testModelExchangePath* = "model_exchange_testing"
  testRoutingKey* = "obj.saved"
  testRoutingKey2* = "obj2.saved"
  testDestinationImp* = "/exchange/$#/$#" % [testModelExchangePath, testRoutingKey]
  testPayloadImpl* = """{"user":"zax","message-id":1}"""
  testMessageId* = "ok"

var
  testPayload* = testPayloadImpl
  testDestination* = testDestinationImp
  testExposeMessageId* = true

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
    etConnect, etSend, etDisconnect, etSubscribe, etAck, etNack, etDrain,
    etCallback, etSubscriber, etOnConnect
  Event* = object
    eventType*: EventType
    values*: seq[string]

proc event*(eventType: EventType, values: varargs[string]): Event =
  Event(
    eventType: eventType,
    values: @values
  )

proc `[]`*(r: StompResponse, key: string): string =
  case key
  of "destination":
    result = testDestination
  of "message-id":
    if not testExposeMessageId:
      result = nil
    else:
      result = testMessageId
  of "server":
    result = "RabbitMQ/3.6.15"
  else:
    result = nil

proc `$`*(c: StompClient): string =
  result = "Client " & c.uri

proc `$`*(r: StompResponse): string =
  result = "Response" & r.payload

proc newStompClient*(socket: Socket, uri: string): StompClient =
  result = StompClient(
    uri: uri
  )

var testEventLog* = newSeq[Event]()
proc resetEventLog*() =
  testEventLog.setLen(0)

proc connect*(c: StompClient) =
  testEventLog.add event(etConnect, c.uri)
  c.connected = true
  if not c.connectedCallback.isNil:
    let stompResponse = StompResponse(
      payload: testPayload
    )
    c.connectedCallback(c, stompResponse)

proc ack*(c: StompClient, id: string) =
  testEventLog.add event(etAck, id)

proc nack*(c: StompClient, id: string) =
  testEventLog.add event(etNack, id)

proc subscribe*(
  c: StompClient,
  destination,
  ack = "",
  id = "",
  headers: seq[Header] = @[]
) =
  testEventLog.add event(etSubscribe, destination)

proc disconnect*(c: StompClient) =
  testEventLog.add event(etDisconnect, c.uri)
  c.connected = false

proc waitForMessages*(c: StompClient) =
  testEventLog.add event(etDrain)

  let stompResponse = StompResponse(
    payload: testPayload
  )

  if not c.messageCallback.isNil:
    testEventLog.add event(etCallback, testPayload)
    c.messageCallback(c, stompResponse)

proc send*(c: StompClient, destination, payload, content_type: string) =
  testEventLog.add event(etSend, destination, payload)
