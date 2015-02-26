<img align="right" src="https://github.com/vektra/vega/blob/master/doc/penrose.png">

Vega
=======

A distributed mailbox system for building distributed systems.

* Version: **0.4**
* [Announcement blog post](http://vektra.com/blog/vega)

# Features

* Distributed by default operations
* No centralized point of failure
* All messages are durable, stored to disk via LevelDB
* Deliver to components is at-least-once
  * Components are required to ack or nack each message
* Agents on each machine use routing to discover how to deliver to mailboxes
  * Consul provides a consistent, distributed routing table
* Native Go API
* [Simple HTTP interface](https://github.com/vektra/vega/blob/master/doc/HTTP_API.md)

## Eventual Features

* AMQP API support
* STOMP API support

# Requirements

* Consul 0.5.0 or greater

# Installation

## Linux

* [i386](https://bintray.com/artifact/download/evanphx/vega/vega-0.3-linux-386.zip)
* [amd64](https://bintray.com/artifact/download/evanphx/vega/vega-0.3-linux-amd64.zip)

## Darwin

* [amd64](https://bintray.com/artifact/download/evanphx/vega/vega-0.3-darwin-amd64.zip)

## Via Go

* `go get github.com/vektra/vega/cmd/vegad`

# Focus

The focus of Vega is to provide a resilient, trustworthy message delivery system.

Within an organization, it would form the backbone for how components communicate
with each other.

At present, the focus is not on providing an ultra fast message passing system.
There are many projects that look to provide this and generally when 
an ultra fast message system is needed, you want different guarantees on messages.
For instance, you're ok losing messages at the expense of continuning to run
quickly. Reliable and resilient is the focus because those are the aspects of a
backbone communication system that are most important.


# What is a distributed mailbox system?

It's common when building a distributed system (any distributed system) that
components running on different machines need to talk to each other. A common
way to model that communication is via passing messages between the components.
Perhaps you've heard of this idea, it's pretty cool. Once a team decides they
want to exchange messages between components, they have to figure out how
to do that.

Also check out our [Mailbox vs Queue thoughts](https://github.com/vektra/vega/blob/master/doc/MAILBOX_VS_QUEUE.md)

## Queue Brokers

A common approach is to use a central service that all components talk to.
This service typically provides named queues which components put messages
into and pull them out from. This central service acts as an arbitrator
for all the queues.

A centralized service that all components talk to means that the reliability
of your whole system is now the reliability of this single, central service.
It's typical for these queue brokers to be difficult to configure in a way
that provides high availability as well, only making it more difficult
for users to have confidence in this very important service.

## Brokerless Queues

Another approach is to use software within each component that allows
the components to talk directly to each other. This means there is no central
broker that the reliability is pinned on, which is great.

A common issue with this approach is that each component is now significantly
more complicated. Each component has to keep track of all other components
that are interested in its messages. A bigger issue is how the availability
of a component affect the availability of its messages. If a component crashes
or needs to restart, it could lose any messages it hadn't fully sent out as well
any component that wants to talk to it might timeout sending while the component
is unavailable. All those problems can be solved, they just add complexity to
a component. For some applications, that's a problem worth solving, but for many
it's not.

## Distributed Mailbox

Vega attempts to reconcile these various needs by providing a system with the following characteristics:

* Named mailboxes that messages are pushed and pulled from
* Each machine runs their own broker
* Components talk to the broker on their machine only
* Brokers use a distributed routing table (provided by Consul only right now) to pass messages between machines

This means that they differ heavily from a centralized queue system in that 2 
different components can not share a mailbox. Thus a Vega mailbox
can not be used as a work queue directly (though it could be used to build one).


