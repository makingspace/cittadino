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

template nackAndReturn(c: StompClient, id: string): untyped =
  c.nack(id)
  return

proc constructMessageCallback(pubsub: PubSub): StompCallback =
  proc onMessageCallback(c: StompClient, r: StompResponse) {.closure.} =
    let
      id = r["message-id"]
      destination = r["destination"]

    if id.isNil:
      logging.error "Could not extract message-id; can not ACK, will not continue.\nResp: $#" % [
        $r
      ]
      return
    if "/" notin destination:
      logging.error "Malformed message destination: " & destination
      nackAndReturn(c, id)

    let routingKey = destination.rsplit("/", maxsplit=1)[1]
    var hasMatched = false

    for pattern, callbacks in pubsub.subscribers.values:
      if routingKey.match(pattern):
        hasMatched = true
        try:
          let responseJson = r.payload.parseJson()

          for callback in callbacks:
            callback(responseJson)
        except JsonParsingError:
          logging.error "Expected JSON in payload to $#: $#" % [
            destination, r.payload
          ]
          nackAndReturn(c, id)

    c.ack(id)
    if not hasMatched:
      logging.error "Expected $# in subscribers." % routingKey

  result = onMessageCallback

proc newPubSub*(stompUrl, modelExchangeName: string, nameSpace = "", ): PubSub =
  ## Create a new PubSub object. Given the URL of a STOMP server and the name
  ## of the exchange to listen to, will create and manage a STOMP connection,
  ## optionally suffixed with ``nameSpace``.
  runnableExamples:
    var pubsub = newPubSub("stomp://user:user@192.168.111.222/", "")

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

proc `connectedCallback=`*(pubsub: PubSub, callback: StompCallback) =
  ## Set the callback procedure to be called whenever ``pubsub`` connects to
  ## its server.
  pubsub.client.connectedCallback = callback

proc `heartbeatCallback=`*(pubsub: PubSub, callback: StompCallback) =
  ## Set the callback procedure to be called whenever ``pubsub`` receives its
  ## heartbeat from the STOMP server.
  pubsub.client.heartbeatCallback = callback

proc `errorCallback=`*(pubsub: PubSub, callback: StompCallback) =
  ## Set the callback procedure to be called whenever ``pubsub`` receives a
  ## error from the STOMP server.
  pubsub.client.errorCallback = callback

proc exchangePath(pubsub: PubSub): string =
  let exchangeName = if pubsub.nameSpace == "": pubsub.exchangeName else: "$#_$#" % [
    pubsub.exchangeName, pubsub.nameSpace
  ]

  return "/exchange/" & exchangeName

proc subscribe*(pubsub: var PubSub, topic: string, callback: PubSubCallback) =
  ## Register a callback procedure against a subscription pattern. ``callback``
  ## whenever a message is received whose destination matches against
  ## ``topic``.
  ##
  runnableExamples:
    import json
    proc handler(json: JsonNode) =
      echo "Got a new message!"

    var pubsub = newPubSub("stomp://user:user@192.168.111.222/", "")

    pubsub.subscribe("user.*", handler)

  if topic in pubsub.subscribers:
    if callback notin pubsub.subscribers[topic].callbacks:
      pubsub.subscribers[topic].callbacks.add(callback)
  else:
    pubsub.subscribers[topic] = (re(topic), @[callback])

  if not pubsub.client.connected:
    pubsub.client.connect()
  pubsub.client.subscribe(
    "$#/$#" % [pubsub.exchangePath, topic],
    "client-individual",
    @[("durable", "true"), ("auto-delete", "false")]
  )

proc run*(pubsub: PubSub) =
  ## Run forever, listening for messages from the STOMP server. When one is
  ## received, any handlers registered with ``subscribe()`` will be called on
  ## the message's payload.
  ##
  if not pubsub.client.connected:
    pubsub.client.connect()

  defer: pubsub.client.disconnect()

  pubsub.client.waitForMessages()

proc payload(obj: JsonNode): string =
  result = ""
  let jsonOut = %*{"object": obj}
  toUgly(result, jsonOut)

proc publishModelEvent*(pubsub: PubSub, model_name, event_name: string, obj: JsonNode) =
  ## Publish an event to ``pubsub``'s model event exchange. It will be routed
  ## towards ``model_name``.``event_name``, eg, ``user.saved``. ``obj`` will be
  ## serialized into the message payload.
  ##
  runnableExamples:
    import json
    var
      pubsub = newPubSub("stomp://user:user@192.168.111.222/", "")
      jsonObj = %*{"id": 1}

    pubsub.publishModelEvent("user", "saved", jsonObj)

  pubsub.client.connect()
  defer:
    pubsub.client.disconnect()

  pubsub.client.send(
    "$#/$#.$#" % [pubsub.exchangePath, model_name, event_name],
    payload(obj),
    "application/json"
  )
