# Comparing Vega with other systems

### Vega vs Queue with a one node installation (single)

A "one node installation" means that all connections to the message manager are to localhost, no other nodes running clients are used.

* Both models allow multilpe consumers of the same mailbox/queue
* Both models allow clients to ack messages to elimitate losing messages on client crashes
* Many queue systems are optimized for very high message throughput by only storing messages in memory
  * Vega mailboxs always write all messages to disk for improved reliability, but reduced throughput
* All in all, in a single node installation, vega and queue systems are pretty similar.

### Vega vs Queue with a multiple node installation (vs-queue)

* Vega expects each node using the installation to run vegad
  * Said a different way, each node running any process that produces or consumes messages runs vegad.
  * Queue systems run a broken on small number of nodes that make up the "cluster"
* If the same mailbox name is declared on multiple nodes, this creates distinct mailboxes.
  * Delivering to this name will result in the message being delivered to all mailboxs of that name. This means consumers on every node that declare that mailbox will see the message.
  * The byproduct of this is that a declared name on can not be used as a work queue because there is not single delivery in a distributed environment.

## RabbitMQ

* Both single and vs-queue claims above apply.
