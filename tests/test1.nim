import unittest, json, sequtils, strutils
import pubnub
import stomp

const
  testUrl = "stomp://localhost/"
  testModelExchangeName = "model_exchange"
  testNameSpace = "testing"
  testModelExchangePath = "model_exchange_testing"
  testRoutingKey = "obj.saved"

suite "client tests":

  setUp:
    var pubsub = newPubSub(testUrl, testModelExchangeName, testNameSpace)
    eventLog.setLen(0)

  proc checkDestination(destination: string) =
    let components = destination.split("/")
    check:
      "" == components[0]  # leading slash
      "exchange" == components[1]
      testModelExchangePath == components[2]
      testRoutingKey == components[3]

  test "publish":
    let obj = %*{
      "id": 1
    }
    pubsub.publishModelEvent("obj", "saved", obj)

    let eventTypes = eventLog.mapIt(it.eventType)

    check @[etConnect, etSend, etDisconnect] == eventTypes

    let
      sendEvent = eventLog[1]
      payload = sendEvent.values[1]

    check $obj == payload

    let destination = sendEvent.values[0]
    checkDestination(destination)

  test "subscribe":
    proc handler(json: JsonNode) =
      discard

    pubsub.subscribe(testRoutingKey, handler)

    let eventTypes = eventLog.mapIt(it.eventType)

    check @[etConnect, etSubscribe] == eventTypes

    let
      subscribeEvent = eventLog[1]
      destination = subscribeEvent.values[0]

    checkDestination(destination)
