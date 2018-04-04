import random, json, strutils
from os import paramStr
from logging import nil
import cittadino

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
