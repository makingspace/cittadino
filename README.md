# cittadino

cittadino is a simple, small library that implements a pubsub system over STOMP.

It exposes a single main object type, PubSub, with its constructor, newPubSub(), which provides two main procs: publishModelEvent() and subscribe(). These procedures take care of publishing and subscribing, respectively. Subscriber procedures can be registered with subscribe() to certain AMQP/STOMP-style topic patterns, and arbitrary JSON messages can be sent to routing keys with publishModelEvent().

In order process incoming events, call run() inside of a process. That process will loop and block, waiting for incoming messages. Any topics that match the patterns that were registered with subscribe() will have their messages passed to the handler procedures.

See the [docs](https://makingspace.github.io/cittadino/) for more information.
