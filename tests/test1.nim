import unittest, json, sequtils, strutils
import cittadino
import stomp

proc checkDestination(destination: string) =
  let components = destination.split("/")
  check:
    "" == components[0]  # leading slash
    "exchange" == components[1]
    testModelExchangePath == components[2]
    testRoutingKey == components[3]

proc eventTypes(): seq[EventType] =
  testEventLog.mapIt(it.eventType)

suite "pubsub tests":

  setUp:

    proc onConnect(c: StompClient, r: StompResponse) =
      testEventLog.add event(etOnConnect)

    proc handler(json: JsonNode) =
      testEventLog.add event(etSubscriber)

    proc handler2(json: JsonNode) =
      handler(json)

    var pubsub = newPubSub(testUrl, testModelExchangeName, testNameSpace)
    pubsub.connectedCallback = onConnect

    resetEventLog()

  test "publish":
    let obj = %*{
      "id": 1
    }
    pubsub.publishModelEvent("obj", "saved", obj)

    check @[etConnect, etOnConnect, etSend, etDisconnect] == eventTypes()

    let
      sendEvent = testEventLog[2]
      payload = sendEvent.values[1]

    check $(%*{"object": obj}) == payload

    let destination = sendEvent.values[0]
    checkDestination(destination)

  test "subscribe":
    pubsub.subscribe(testRoutingKey, "bookingSub1"):
      handler(update)

    check @[etConnect, etOnConnect, etSubscribe] == eventTypes()

    let
      subscribeEvent = testEventLog[2]
      destination = subscribeEvent.values[0]

    checkDestination(destination)

  test "message callback with no subscribers":
    pubsub.run()

    check @[
      etConnect,
      etOnConnect,
      etDrain,
      etCallback,
      etAck,
      etDisconnect
    ] == eventTypes()

    let
      connectEvent = testEventLog[0]
      ackEvent = testEventLog[4]
      disconnectEvent = testEventLog[^1]

    check:
      connectEvent.values[0] == testUrl
      ackEvent.values[0] == testMessageId
      disconnectEvent.values[0] == testUrl

  test "message callback with one subscriber":
    pubsub.subscribe(testRoutingKey, handler, "bookingSub1")
    pubsub.run()

    check @[
      etConnect,
      etOnConnect,
      etSubscribe,
      etDrain,
      etCallback,
      etSubscriber,
      etAck,
      etDisconnect
    ] == eventTypes()

  test "message callback with two subscribers":
    pubsub.subscribe(testRoutingKey, handler, "bookingSub1")
    pubsub.subscribe(testRoutingKey, handler2, "bookingSub2")
    pubsub.run()

    check @[
      etConnect,
      etOnConnect,
      etSubscribe,
      etSubscribe,
      etDrain,
      etCallback,
      etSubscriber,
      etSubscriber,
      etAck,
      etDisconnect
    ] == eventTypes()

  test "message subscription is idempotent":
    pubsub.subscribe(testRoutingKey, handler, "bookingSub1")
    # Subscribe twice.
    pubsub.subscribe(testRoutingKey, handler, "bookingSub1")
    pubsub.run()

    check @[
      etConnect,
      etOnConnect,
      # Two subscriptions...
      etSubscribe,
      etSubscribe,
      etDrain,
      etCallback,
      # But only one subscriber callback.
      etSubscriber,
      etAck,
      etDisconnect
    ] == eventTypes()

  test "message subscription with other subscriber":
    pubsub.subscribe(testRoutingKey2, handler, "bookingSub1")
    pubsub.run()

    check @[
      etConnect,
      etOnConnect,
      etSubscribe,
      etDrain,
      etCallback,
      etAck,
      etDisconnect
    ] == eventTypes()

suite "non-JSON pubsub tests":

  setUp:
    proc handler(json: JsonNode) =
      testEventLog.add event(etSubscriber)

    var pubsub = newPubSub(testUrl, testModelExchangeName, testNameSpace)
    resetEventLog()

    let
      nonJsonPayload = "non-json"
      originalPayload = testPayload

    testPayload = nonJsonPayload

  tearDown:
    testPayload = originalPayload

  test "handle non-JSON with no subscriber":
    pubsub.run()

    check @[
      etConnect,
      etDrain,
      etCallback,
      etAck,
      etDisconnect
    ] == eventTypes()

    check testEventLog[2].values[0] == nonJsonPayload

  test "handle non-JSON with one subscriber":
    pubsub.subscribe(testRoutingKey, handler, "bookingSub1")
    pubsub.run()

    check @[
      etConnect,
      etSubscribe,
      etDrain,
      etCallback,
      etAck,
      etDisconnect
    ] == eventTypes()

    check testEventLog[3].values[0] == nonJsonPayload

suite "malformed destination pubsub tests":

  setUp:
    var pubsub = newPubSub(testUrl, testModelExchangeName, testNameSpace)
    resetEventLog()

    let
      malformedDestination = "oops"
      originalDestination = testDestination

    testDestination = malformedDestination

  tearDown:
    testDestination = originalDestination

  test "handle non-JSON":
    pubsub.run()

    check @[
      etConnect,
      etDrain,
      etCallback,
      etAck,
      etDisconnect
    ] == eventTypes()

suite "missing message-id pubsub tests":

  setUp:
    var pubsub = newPubSub(testUrl, testModelExchangeName, testNameSpace)
    resetEventLog()

    let originalTestExposeMessageId = testExposeMessageId

    testExposeMessageId = false

  tearDown:
    testExposeMessageId = originalTestExposeMessageId

  test "handle missing message Id":
    pubsub.run()

    check @[
      etConnect,
      etDrain,
      etCallback,
      etDisconnect
    ] == eventTypes()

