## cittadino: a simple, opinionated pubsub framework
## =================================================
##
## *cittadino* is a simple, small library that implements a pubsub system over
## STOMP.
##
## It exposes a single main object type, ``PubSub``, with its constructor,
## ``newPubSub()``, which provides two main procs: ``publishModelEvent()`` and
## ``subscribe()``. These procedures take care of publishing and subscribing,
## respectively. Subscriber procedures can be registered with ``subscribe()``
## to certain AMQP/STOMP-style topic patterns, and arbitrary JSON messages can
## be sent to routing keys with ``publishModelEvent()``.
##
## In order process incoming events, call ``run()`` inside of a process. That
## process will loop and block, waiting for incoming messages. Any topics that
## match the patterns that were registered with ``subscribe()`` will have their
## messages passed to the handler procedures.
##
import stomp, net, json, strutils, tables, re
from logging import nil

type
  PubSubCallback = proc(update: JsonNode)
  StompCallback = proc(c: StompClient, r: StompResponse) {.closure.}
  Subscriber = tuple[pattern: Regex, callback: PubSubCallback]
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

template ackAndReturn(c: StompClient, id: string): untyped =
  c.ack(id)
  return

template nackAndReturn(c: StompClient, id: string): untyped =
  c.nack(id)
  return

type StopProcessingAck* = object of Exception ## \
  ## Regardless of callback order, when this exception is caught, ACK, return
  ## and don't call any more exceptions.

type StopProcessingNack* = object of Exception ## \
  ## Regardless of callback order, when this exception is caught, NACK, return
  ## and don't call any more exceptions.

type SkipSubscriber* = object of Exception ## \
  ## Skip the current subscriber callback and continue with the next one, or
  ## ACK if there are no other subscribers.

proc constructMessageCallback(pubsub: PubSub): StompCallback =
  proc onMessageCallback(c: StompClient, r: StompResponse) {.closure.} =
    let
      id = r["message-id"]
      destination = r["destination"]

    if id.len == 0:
      logging.error "Could not extract message-id; can not ACK, will not continue.\nResp: $#" % [
        $r
      ]
      return
    if "/" notin destination:
      logging.error "Malformed message destination: " & destination
      ackAndReturn(c, id)

    let routingKey = destination.rsplit("/", maxsplit=1)[1]
    var hasMatched = false

    for pattern, callback in pubsub.subscribers.values:
      if routingKey.match(pattern):
        hasMatched = true
        try:
          let responseJson = r.payload.parseJson()
          callback(responseJson)
        except JsonParsingError:
          logging.error "Expected JSON in payload to $#: $#" % [
            destination, r.payload
          ]
          ackAndReturn(c, id)
        except StopProcessingAck:
          ackAndReturn(c, id)
        except StopProcessingNack:
          nackAndReturn(c, id)
        except SkipSubscriber:
          continue

    c.ack(id)
    if not hasMatched:
      logging.error "Expected $# in subscribers." % routingKey

  result = onMessageCallback

proc newPubSub*(stompUrl, modelExchangeName: string, nameSpace = "", ): PubSub =
  ## Create a new PubSub object. Given the URL of a STOMP server and the name
  ## of the exchange to listen to, will create and manage a STOMP connection,
  ## optionally suffixed with ``nameSpace``.
  ##
  ## The URL should be a fully qualified `STOMP` URI: starting with
  ## ``stomp://`` or ``stomp+ssl://`` including necessary user/password
  ## information, including a ``vhost`` at the end (in the examples, the
  ## default ``/``), and finally ending with query parameters to encode
  ## connection options.
  ##
  ## Currently the Nim STOMP library understands a single query parameter:
  ## ``heartbeat=<interval>``, which will request a heartbeat from the STOMP
  ## server every ``interval`` seconds.
  runnableExamples:
    var pubsub = newPubSub("stomp://user:user@192.168.111.222/", "model_exchange")
  runnableExamples:
    var pubsub = newPubSub("stomp://user:user@192.168.111.222/?heartbeat=10", "amq_ex")
  runnableExamples:
    var pubsub = newPubSub("stomp+ssl://user:user@192.168.111.222/", "my_exchange")

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

proc subscribe*(
  pubsub: var PubSub,
  topic: string,
  callback: PubSubCallback,
  subscriberName: string,
) =
  let key = topic & "%" & subscriberName
  pubsub.subscribers[key] = (re(topic), callback)

  if not pubsub.client.connected:
    pubsub.client.connect()

  pubsub.client.subscribe(
    "$#/$#" % [pubsub.exchangePath, topic],
    ack = "client-individual",
    id = "[$#]$#" % [pubsub.nameSpace, subscriberName],
    headers = @[("durable", "true"), ("auto-delete", "false")]
  )

template subscribe*(
  pubsub: var PubSub,
  topic: string,
  subscriberName: string,
  body: untyped
): typed =
  ## Register a callback procedure against a subscription pattern. ``callback``
  ## whenever a message is received whose destination matches against
  ## ``topic``.
  ##
  ## Destinations are declared with ``durable:true``, which specifies that if
  ## the broker is restarted, the queue should be retained. By default they are
  ## also created with ``auto-delete:false``, which specifies that the queue
  ## should not be destroyed if there is no active consumer (ie, published
  ## messages will still be routed to the queue and consumed when ``run()`` is
  ## called again).
  runnableExamples:
    import json
    var pubsub = newPubSub("stomp://user:user@192.168.111.222/", "model_exchange")

    pubsub.subscribe("user.*", "sampleHandler"):
      echo "Got a new message!"

  proc callback(update: JsonNode) {.gensym.} =
    var update {.inject.} = update
    body

  subscribe(pubsub, topic, callback, subscriberName)

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
      pubsub = newPubSub("stomp://user:user@192.168.111.222/", "model_exchange")
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
