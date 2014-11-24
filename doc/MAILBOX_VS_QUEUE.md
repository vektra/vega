In the world of messaging, most people think about queues. It is the most common
model implemented because it represents a commonly understood way of considering
a shared resource: any one can put things in and once someone taks something out,
it's gone.

The important aspect of this thinking is the shared resource. A big point of
emphasis around messaging passing architectures it disconnecting the sender
and the receiver. The queue represents the middle-man: taking from the sender
as fast as it can, and giving out to the receiver (or receivers) as they request
messages.

The true aim of messaging passing is provide an asyncronious layer between the
sender and the receiver. Code that sends a request and then blocks, waiting for
a response, is less resiliant to errors created by networks. Additionally,
stateful request/response connections lend themselves to implicit context.
This context, in the face of errors and changing topologies, is easily lost
and unrecoverable. This means the operations being done related to the
request/response connection can be incoherent and cause confusion within
the system.

Codifying operations within context-less messages, on the other, has 2
benefits:

1. The messages can be acted upon by any receiver that subscribe to them,
resulting in a very easy ability to add addition capacity.
2. The message transmission is flexible, even lending itself to trivial
retransmission. If the sender wants the receiver to get the message at some
point and isn't going to wait around, an intermediate can do whatever is
necessary to deliver the message. This might be waiting for a receiver
to come available if none are there or even starting new receivers if
the intermediate has that ability.

Looking back at our original point of comparing message queues with message
mailboxes, both systems provide the ability to decouple the sending
code from the receiving code. Queue systems go an extra level and provide
ingrained ability to have many receivers of a single queue where each
message in the queue is only given out to a single receiver. This is commonly
known as a "work queue" and is quite valuable. Many problems in can
be tackled easily by using a work queue and they make it very easy to add
additional capacity.

But it would be wrong to say that a work queue system requires a message
queue broker. There are plenty of models for a work queue that don't utilize
a queue located in a broker.

A mailbox system does not provide an integration work queue, because it
concerns itself with a level below a message queue, namely the mechanism
of how to deliver a single message into a named mailbox. A mailbox system
may model it's semantics to allow a mailbox to be used as a work queue,
but that mailbox model is flexible enough that this is not a requirement
(nor is it a typical expectation of users).

Given this increased flexibility, a mailbox system can archieve better
distributed semantics that a queue system because there is no requirement
of linearization into a named queue. A distributed mailbox system need
only provide a way to map a mailbox name to a node to provide availability.
This means that it's possible to run a mailbox daemon on each node, with
each node using a routing protocol to know which mailbox lives on which node.
The routing protocol distribution mechanism can be very robust and effectively
mean there is no single point of a failure within a mailbox system. If 8 of 10
nodes go offline, the other 2 can still happily send messages between eachother.
