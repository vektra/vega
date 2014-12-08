# HTTP API

The builtin HTTP API provides a simple way to interface with the local vegad
agent.

## Operations

* POST /mailbox/:name
  * Declare (i.e. create if does not exist) a mailbox. All mailboxes must be declared before they can be used.
* DELETE /mailbox/:name
  * Abandon a mailbox. Only mailboxes on the local agent may be abondoned.
* PUT /mailbox/:name
  * Add a new message to a mailbox. The mailbox may be remote, in which case the agent will route it to the proper agent.
  * Passing `application/x-msgpack` in the `Content-Type` header tells the server to decode the message as MessagePack format. `Content-Type` defaults to JSON.
* GET /mailbox/:name
  * Pull a message out of a mailbox. The mailbox must be a locally declared mailbox as Vega does not allow reading a mailbox on another agent.
  * Use the `wait` parameter to use a long poll request. The value is how long the query will potentially block for. Examples are "10s" for 10 seconds and "5m" for 5 minutes. If there is no message within the time limit, a 204 is returned.
  * Passing a `lease` parameter will set a lease on the message. See below for information on message leases.
  * Passing `application/x-msgpack` in the `Accept` header will result in the body being in MessagePack format rather than JSON.
* DELETE /message/:id
  * Acknowledge a message previously pulled from a mailbox. This or PUT must be done to all messages in order for Vega to know the message has been handled.
* PUT /messages/:id
  * Indicate that the message should be returned to it's mailbox because the component could not handle it.

## Formats

PUTing a message into a mailbox supports 3 different formats the message may
be in: JSON, MessagePack, or URLencoded. For JSON and MessagePack, the format of a message is:

```js
{
  "headers": {},                // Any user specific headers
  "content_type": "foo/bar",    // MIME content type
  "priority": 0,                // 0 to 9
  "correlation_id": "xxyyzz11", // A related id
  "reply_to": "aabbcc2",        // A mailbox to reply to
  "message_id": "a-b-c-d",      // A system defined message id
  "timestamp": "2006-01-02T15:04:05Z07:00", // When the message was sent
  "type": "my_cool_message",    // user defined type identifier
  "user_id": "evan"             // user that created the message
  "app_id": "test"              // component that created the message

  "body": "aGVsbG8="            // base64 encoded message
}
```

When PUTing a message, `message_id` must be empty and timestamp may be empty.

Other than `message_id` and `timestamp`, the system does read the message, it
simply passes them through. Many people will notice these are the same fields
present in an AMQP message. That's because AMQP defines a great set of very
common fields that are used to express information about a message. This keeps
the messages simply and doesn't bloat the message body with metadata info, like
type for instance.

For URLencoded (`application/x-www-form-urlencoded`), the message fields are
set from the encoded keyed values. Ie, `curl -d "body=hello" localhost:8477/mailbox/foo`.

## Leases

When using a connection oriented protocol (currently that is only the native Go API)
the server can detect when a client disconnects and automatically NACK any message
that had not been ACKed yet.

When using a request oriented protocol like HTTP, there is no ability to do that.
So instead a separate `lease` parameter is used. When a message is returned
to a client, a timer begins, set to the value of the lease. If the message is
not ACKd or NACKd before the timer expires, the message is automatically NACKd
because the system assumes the client crashed and the message was not handled.

The default lease is 5 minutes. The format is the same as `wait`, for example `10s`
for 10 seconds. The default value is very conservative to allow HTTP clients
a lot of leeway. Clients should generally set it to value that makes sense
for their usage.
