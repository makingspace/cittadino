import stomp, net, json, strutils, tables, re
from logging import nil

type
  PubSubCallback = proc(update: JsonNode)
  StompCallback = proc(c: StompClient, r: StompResponse)
  Subscriber = tuple[pattern: Regex, callbacks: seq[PubSubCallback]]
  PubSub = ref object
    client: StompClient
    exchangeName, nameSpace: string
    subscribers: TableRef[string, Subscriber]

proc defaultOnConnected(c: StompClient, r: StompResponse) =
  logging.info "Connected to $# server at $#" % [r["server"], $c.uri]

proc defaultOnHeartbeat(c: StompClient, r: StompResponse) =
  logging.debug "Heartbeat: ", c.lastMsgTime

proc defaultOnError(c: StompClient, r: StompResponse) =
  logging.error "Error: ", r

proc constructMessageCallback(pubsub: PubSub): StompCallback =
  proc onMessageCallback(c: StompClient, r: StompResponse) {.closure.} =
    let id = r["message-id"]

    defer:
      c.ack(id)

    let
      destination = r["destination"]
      routingKey = destination.rsplit("/", maxsplit=1)[1]

    var hasMatched = false
    for pattern, callbacks in pubsub.subscribers.values:
      if routingKey.match(pattern):
        hasMatched = true
        let responseJson = r.payload.parseJson()

        for callback in callbacks:
          callback(responseJson)

    if not hasMatched:
      logging.error "Expected $# in subscribers." % routingKey

  result = onMessageCallback

proc newPubSub*(stompUrl, modelExchangeName: string, nameSpace = "", ): PubSub =
  let
    socket = newSocket()
    client = newStompClient(socket, stompUrl)

  result = PubSub(
    client: client,
    exchangeName: modelExchangeName,
    nameSpace: nameSpace,
    subscribers: newTable[string, Subscriber](),
  )
  result.client.connectedCallback = defaultOnConnected
  result.client.heartbeatCallback = defaultOnHeartbeat
  result.client.errorCallback = defaultOnError
  result.client.messageCallback = constructMessageCallback(result)

proc exchangePath(pubsub: PubSub): string =
  let exchangeName = if pubsub.nameSpace == "": pubsub.exchangeName else: "$#_$#" % [
    pubsub.exchangeName, pubsub.nameSpace
  ]

  return "/exchange/" & exchangeName

proc subscribe*(pubsub: var PubSub, topic: string, callback: PubSubCallback) =
  if topic in pubsub.subscribers:
    pubsub.subscribers[topic].callbacks.add(callback)
  else:
    pubsub.subscribers[topic]= (re(topic), @[callback])

  pubsub.client.connect()
  pubsub.client.subscribe(
    "$#/$#" % [pubsub.exchangePath, topic],
    "client-individual",
    @[("durable", "true"), ("auto-delete", "false")]
  )

proc run*(pubsub: PubSub) =
  if not pubsub.client.connected:
    pubsub.client.connect()
    defer:
      pubsub.client.disconnect()

  pubsub.client.waitForMessages()

proc publishModelEvent*(pubsub: PubSub, model_name, event_name: string, obj: JsonNode) =
  pubsub.client.connect()
  defer:
    pubsub.client.disconnect()

  var payload = ""
  toUgly(payload, obj)

  pubsub.client.send(
    "$#/$#.$#" % [pubsub.exchangePath, model_name, event_name],
    payload,
    "application/json"
  )

when isMainModule:
  from os import paramStr
  import random

  randomize()

  var L = logging.newConsoleLogger()
  logging.addHandler(L)

  var pubsub = newPubSub("stomp://user:user@192.168.111.222/?heartbeat=10", "model_event_exchange")

  let cmd = paramStr(1)
  case cmd
  of "run":
    const
      topic1 = "user.*"
      topic2 = "user.saved"
      topic3 = "llama.*"

    proc callback(j: JsonNode) =
      let user = j["user"].getStr("default")
      echo "[$1] Got user: $2!" % [topic1, user]

    proc callback2(j: JsonNode) =
      let user = j["user"].getStr("default")
      echo "[$1] Got user: $2... Again!" % [topic2, user]

    proc callback3(j: JsonNode) =
      echo "[$1] this is not about users." % topic3

    pubsub.subscribe(topic1, callback)
    pubsub.subscribe(topic2, callback2)
    pubsub.subscribe(topic3, callback3)

    pubsub.run()

  of "send":
    const NAMES = [
      "James", "Mary",
      "John", "Patricia",
      "Robert", "Jennifer",
      "Michael", "Elizabeth",
      "William", "Linda",
      "David", "Barbara",
      "Richard", "Susan",
      "Joseph", "Jessica",
      "Thomas", "Margaret",
      "Charles", "Sarah",
      "Christopher", "Karen",
      "Daniel", "Nancy",
      "Matthew", "Betty",
      "Anthony", "Lisa",
      "Donald", "Dorothy",
      "Mark", "Sandra",
      "Paul", "Ashley",
      "Steven", "Kimberly",
      "Andrew", "Donna",
      "Kenneth", "Carol"
    ]

    let obj = %*{
      "user": rand(NAMES),
      "extra": true
    }
    pubsub.publishModelEvent("user", "saved", obj)
