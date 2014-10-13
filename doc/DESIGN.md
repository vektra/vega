# Design Considerations

## Simple

Vega is designed to be a simple, medium-level building block. As such it lacks some sophisticated
features by design.

## Distributed

The expectation is that single vega installation is used by many nodes simulatiously.

## Mailboxes

Vega is really a distributed mailbox system, rather than a queue system.

These mailboxes are generally accessed by one and only one consumer at a time. This means a raw mailbox is not designed to be used as a work queue.

## Per node

Vega is designed so that a vegad daemon is run on ever node is using the system. This means that generally consumers as well as producers talk to a local vegad instead of a remote one.

This has a few important properties:

* Clients have a highly reliable connection to their vegad. This allows client libraries to be simpler because the local vegad takes care of transmission to remote nodes.
* Daemons use a routing table to find other mailboxs within the installation. At present, only consul is supported for this.
* Nodes are assumed to be more reliable than the processes within those nodes. As such, the local vegad supports mechanisms to redeliver messages to processes that crash while dealing with a message (this is commonly know as message acking).
* If a node is lost (cloud provider terminates machine, etc) the messages within the mailboxes that are on that node are lost. This goes back to the fact that vega is not a distributed queue system.
  * Side note: if a node is rebooted (or vegad restarted) the messages and mailboxes are not lost. vegad stores messages for mailboxes on disk.
