// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"fmt"
	"io"
	"time"
)

// DefaultExchange is the default direct exchange that binds every queue by its
// name. Applications can route to a queue using the queue name as routing key.
const DefaultExchange = ""

// Constants for standard AMQP 0-9-1 exchange types.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

var (
	// ErrClosed is returned when the channel or connection is not open
	ErrClosed = &Error{Code: ChannelError, Reason: "channel/connection is not open"}

	// ErrChannelMax is returned when Connection.Channel has been called enough
	// times that all channel IDs have been exhausted in the client or the
	// server.
	ErrChannelMax = &Error{Code: ChannelError, Reason: "channel id space exhausted"}

	// ErrSASL is returned from Dial when the authentication mechanism could not
	// be negotiated.
	ErrSASL = &Error{Code: AccessRefused, Reason: "SASL could not negotiate a shared mechanism"}

	// ErrCredentials is returned when the authenticated client is not authorized
	// to any vhost.
	ErrCredentials = &Error{Code: AccessRefused, Reason: "username or password not allowed"}

	// ErrVhost is returned when the authenticated user is not permitted to
	// access the requested Vhost.
	ErrVhost = &Error{Code: AccessRefused, Reason: "no access to this vhost"}

	// ErrSyntax is hard protocol error, indicating an unsupported protocol,
	// implementation or encoding.
	ErrSyntax = &Error{Code: SyntaxError, Reason: "invalid field or value inside of a frame"}

	// ErrFrame is returned when the protocol frame cannot be read from the
	// server, indicating an unsupported protocol or unsupported frame type.
	ErrFrame = &Error{Code: FrameError, Reason: "frame could not be parsed"}

	// ErrCommandInvalid is returned when the server sends an unexpected response
	// to this requested message type. This indicates a bug in this client.
	ErrCommandInvalid = &Error{Code: CommandInvalid, Reason: "unexpected command received"}

	// ErrUnexpectedFrame is returned when something other than a method or
	// heartbeat frame is delivered to the Connection, indicating a bug in the
	// client.
	ErrUnexpectedFrame = &Error{Code: UnexpectedFrame, Reason: "unexpected frame received"}

	// ErrFieldType is returned when writing a message containing a Go type unsupported by AMQP.
	ErrFieldType = &Error{Code: SyntaxError, Reason: "unsupported table field type"}
)

// internal errors used inside the library
var (
	errInvalidTypeAssertion = &Error{Code: InternalError, Reason: "type assertion unsuccessful", Server: false, Recover: true}
)

// Error captures the code and reason a channel or connection has been closed
// by the server.
type Error struct {
	Code    int    // constant code from the specification
	Reason  string // description of the error
	Server  bool   // true when initiated from the server, false when from this library
	Recover bool   // true when this error can be recovered by retrying later or with different parameters
}

func newError(code uint16, text string) *Error {
	return &Error{
		Code:    int(code),
		Reason:  text,
		Recover: isSoftExceptionCode(int(code)),
		Server:  true,
	}
}

func (e Error) Error() string {
	return fmt.Sprintf("Exception (%d) Reason: %q", e.Code, e.Reason)
}

// Used by header frames to capture routing and header information
type properties struct {
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	Headers         Table     // Application or header exchange table
	DeliveryMode    uint8     // queue implementation use - Transient (1) or Persistent (2)
	Priority        uint8     // queue implementation use - 0 to 9
	CorrelationId   string    // application use - correlation identifier
	ReplyTo         string    // application use - address to to reply to (ex: RPC)
	Expiration      string    // implementation use - message expiration spec
	MessageId       string    // application use - message identifier
	Timestamp       time.Time // application use - message timestamp
	Type            string    // application use - message type name
	UserId          string    // application use - creating user id
	AppId           string    // application use - creating application
	reserved1       string    // was cluster-id - process for buffer consumption
}

// DeliveryMode.  Transient means higher throughput but messages will not be
// restored on broker restart.  The delivery mode of publishings is unrelated
// to the durability of the queues they reside on.  Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart.
//
// This remains typed as uint8 to match Publishing.DeliveryMode.  Other
// delivery modes specific to custom queue implementations are not enumerated
// here.
const (
	Transient  uint8 = 1
	Persistent uint8 = 2
)

// The property flags are an array of bits that indicate the presence or
// absence of each property value in sequence.  The bits are ordered from most
// high to low - bit 15 indicates the first property.
const (
	flagContentType     = 0x8000
	flagContentEncoding = 0x4000
	flagHeaders         = 0x2000
	flagDeliveryMode    = 0x1000
	flagPriority        = 0x0800
	flagCorrelationId   = 0x0400
	flagReplyTo         = 0x0200
	flagExpiration      = 0x0100
	flagMessageId       = 0x0080
	flagTimestamp       = 0x0040
	flagType            = 0x0020
	flagUserId          = 0x0010
	flagAppId           = 0x0008
	flagReserved1       = 0x0004
)

// Queue captures the current server state of the queue on the server returned
// from Channel.QueueDeclare or Channel.QueueInspect.
type Queue struct {
	Name      string // server confirmed or generated name
	Messages  int    // count of messages not awaiting acknowledgment
	Consumers int    // number of consumers receiving deliveries
}

// Publishing captures the client message sent to the server.  The fields
// outside of the Headers table included in this struct mirror the underlying
// fields in the content frame.  They use native types for convenience and
// efficiency.
type Publishing struct {
	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	Headers Table

	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationId   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageId       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserId          string    // creating user id - ex: "guest"
	AppId           string    // creating application id

	// The application specific payload of the message
	Body []byte
}

// Blocking notifies the server's TCP flow control of the Connection.  When a
// server hits a memory or disk alarm it will block all connections until the
// resources are reclaimed.  Use NotifyBlock on the Connection to receive these
// events.
type Blocking struct {
	Active bool   // TCP pushback active/inactive on server
	Reason string // Server reason for activation
}

// DeferredConfirmation represents a future publisher confirm for a message. It
// allows users to directly correlate a publishing to a confirmation. These are
// returned from PublishWithDeferredConfirm on Channels.
type DeferredConfirmation struct {
	DeliveryTag uint64

	done chan struct{}
	ack  bool
}

// Confirmation notifies the acknowledgment or negative acknowledgement of a
// publishing identified by its delivery tag.  Use NotifyPublish on the Channel
// to consume these events.
type Confirmation struct {
	DeliveryTag uint64 // A 1 based counter of publishings from when the channel was put in Confirm mode
	Ack         bool   // True when the server successfully received the publishing
}

// Decimal matches the AMQP decimal type.  Scale is the number of decimal
// digits Scale == 2, Value == 12345, Decimal == 123.45
type Decimal struct {
	Scale uint8
	Value int32
}

// Most common queue argument keys in queue declaration. For a comprehensive list
// of queue arguments, visit [RabbitMQ Queue docs].
//
// [QueueTypeArg] queue argument is used to declare quorum and stream queues.
// Accepted values are [QueueTypeClassic] (default), [QueueTypeQuorum] and
// [QueueTypeStream]. [Quorum Queues] accept (almost) all queue arguments as their
// Classic Queues counterparts. Check [feature comparison] docs for more
// information.
//
// Queues can define their [max length] using [QueueMaxLenArg] and
// [QueueMaxLenBytesArg] queue arguments. Overflow behaviour is set using
// [QueueOverflowArg]. Accepted values are [QueueOverflowDropHead] (default),
// [QueueOverflowRejectPublish] and [QueueOverflowRejectPublishDLX].
//
// [Queue TTL] can be defined using [QueueTTLArg]. That is, the time-to-live for an
// unused queue. [Queue Message TTL] can be defined using [QueueMessageTTLArg].
// This will set a time-to-live for messages in the queue.
//
// [Stream retention] can be configured using [StreamMaxLenBytesArg], to set the
// maximum size of the stream. Please note that stream queues always keep, at
// least, one segment. [Stream retention] can also be set using [StreamMaxAgeArg],
// to set time-based retention. Values are string with unit suffix. Valid
// suffixes are Y, M, D, h, m, s. E.g. "7D" for one week. The maximum segment
// size can be set using [StreamMaxSegmentSizeBytesArg]. The default value is
// 500_000_000 bytes ~= 500 megabytes
//
// Starting with RabbitMQ 3.12, consumer timeout can be configured as a queue
// argument. This is the timeout for a consumer to acknowledge a message. The
// value is the time in milliseconds. The timeout is evaluated periodically,
// at one minute intervals. Values lower than one minute are not supported.
// See the [consumer timeout] guide for more information.
//
// [Single Active Consumer] on quorum and classic queues can be configured
// using [SingleActiveConsumerArg]. This argument expects a boolean value. It is
// false by default.
//
// [RabbitMQ Queue docs]: https://rabbitmq.com/queues.html
// [Stream retention]: https://rabbitmq.com/streams.html#retention
// [max length]: https://rabbitmq.com/maxlength.html
// [Queue TTL]: https://rabbitmq.com/ttl.html#queue-ttl
// [Queue Message TTL]: https://rabbitmq.com/ttl.html#per-queue-message-ttl
// [Quorum Queues]: https://rabbitmq.com/quorum-queues.html
// [feature comparison]: https://rabbitmq.com/quorum-queues.html#feature-comparison
// [consumer timeout]: https://rabbitmq.com/consumers.html#acknowledgement-timeout
// [Single Active Consumer]: https://rabbitmq.com/consumers.html#single-active-consumer
const (
	QueueTypeArg                 = "x-queue-type"
	QueueMaxLenArg               = "x-max-length"
	QueueMaxLenBytesArg          = "x-max-length-bytes"
	StreamMaxLenBytesArg         = "x-max-length-bytes"
	QueueOverflowArg             = "x-overflow"
	QueueMessageTTLArg           = "x-message-ttl"
	QueueTTLArg                  = "x-expires"
	StreamMaxAgeArg              = "x-max-age"
	StreamMaxSegmentSizeBytesArg = "x-stream-max-segment-size-bytes"
	// QueueVersionArg declares the Classic Queue version to use. Expects an integer, either 1 or 2.
	QueueVersionArg = "x-queue-version"
	// ConsumerTimeoutArg is available in RabbitMQ 3.12+ as a queue argument.
	ConsumerTimeoutArg      = "x-consumer-timeout"
	SingleActiveConsumerArg = "x-single-active-consumer"
)

// Values for queue arguments. Use as values for queue arguments during queue declaration.
// The following argument table will create a classic queue, with max length set to 100 messages,
// and a queue TTL of 30 minutes.
//
//	args := amqp.Table{
//		amqp.QueueTypeArg: QueueTypeClassic,
//		amqp.QueueMaxLenArg: 100,
//		amqp.QueueTTLArg: 1800000,
//	}
//
// Refer to [Channel.QueueDeclare] for more examples.
const (
	QueueTypeClassic              = "classic"
	QueueTypeQuorum               = "quorum"
	QueueTypeStream               = "stream"
	QueueOverflowDropHead         = "drop-head"
	QueueOverflowRejectPublish    = "reject-publish"
	QueueOverflowRejectPublishDLX = "reject-publish-dlx"
)

// Table stores user supplied fields of the following types:
//
//	bool
//	byte
//	int8
//	float32
//	float64
//	int
//	int16
//	int32
//	int64
//	nil
//	string
//	time.Time
//	amqp.Decimal
//	amqp.Table
//	[]byte
//	[]interface{} - containing above types
//
// Functions taking a table will immediately fail when the table contains a
// value of an unsupported type.
//
// The caller must be specific in which precision of integer it wishes to
// encode.
//
// Use a type assertion when reading values from a table for type conversion.
//
// RabbitMQ expects int32 for integer values.
type Table map[string]interface{}

func validateField(f interface{}) error {
	switch fv := f.(type) {
	case nil, bool, byte, int8, int, int16, int32, int64, float32, float64, string, []byte, Decimal, time.Time:
		return nil

	case []interface{}:
		for _, v := range fv {
			if err := validateField(v); err != nil {
				return fmt.Errorf("in array %s", err)
			}
		}
		return nil

	case Table:
		for k, v := range fv {
			if err := validateField(v); err != nil {
				return fmt.Errorf("table field %q %s", k, err)
			}
		}
		return nil
	}

	return fmt.Errorf("value %T not supported", f)
}

// Validate returns and error if any Go types in the table are incompatible with AMQP types.
func (t Table) Validate() error {
	return validateField(t)
}

// Sets the connection name property. This property can be used in
// amqp.Config to set a custom connection name during amqp.DialConfig(). This
// can be helpful to identify specific connections in RabbitMQ, for debugging or
// tracing purposes.
func (t Table) SetClientConnectionName(connName string) {
	t["connection_name"] = connName
}

type message interface {
	id() (uint16, uint16)
	wait() bool
	read(io.Reader) error
	write(io.Writer) error
}

type messageWithContent interface {
	message
	getContent() (properties, []byte)
	setContent(properties, []byte)
}

/*
The base interface implemented as:

2.3.5  frame Details

All frames consist of a header (7 octets), a payload of arbitrary size, and a 'frame-end' octet that detects
malformed frames:

	0      1         3             7                  size+7 size+8
	+------+---------+-------------+  +------------+  +-----------+
	| type | channel |     size    |  |  payload   |  | frame-end |
	+------+---------+-------------+  +------------+  +-----------+
	 octet   short         long         size octets       octet

To read a frame, we:

 1. Read the header and check the frame type and channel.
 2. Depending on the frame type, we read the payload and process it.
 3. Read the frame end octet.

In realistic implementations where performance is a concern, we would use
“read-ahead buffering” or “gathering reads” to avoid doing three separate
system calls to read a frame.
*/
type frame interface {
	write(io.Writer) error
	channel() uint16
}

type reader struct {
	r io.Reader
}

type writer struct {
	w io.Writer
}

// Implements the frame interface for Connection RPC
type protocolHeader struct{}

func (protocolHeader) write(w io.Writer) error {
	_, err := w.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1})
	return err
}

func (protocolHeader) channel() uint16 {
	panic("only valid as initial handshake")
}

/*
Method frames carry the high-level protocol commands (which we call "methods").
One method frame carries one command.  The method frame payload has this format:

	0          2           4
	+----------+-----------+-------------- - -
	| class-id | method-id | arguments...
	+----------+-----------+-------------- - -
	   short      short    ...

To process a method frame, we:
 1. Read the method frame payload.
 2. Unpack it into a structure.  A given method always has the same structure,
    so we can unpack the method rapidly.  3. Check that the method is allowed in
    the current context.
 4. Check that the method arguments are valid.
 5. Execute the method.

Method frame bodies are constructed as a list of AMQP data fields (bits,
integers, strings and string tables).  The marshalling code is trivially
generated directly from the protocol specifications, and can be very rapid.
*/
type methodFrame struct {
	ChannelId uint16
	ClassId   uint16
	MethodId  uint16
	Method    message
}

func (f *methodFrame) channel() uint16 { return f.ChannelId }

/*
Heartbeating is a technique designed to undo one of TCP/IP's features, namely
its ability to recover from a broken physical connection by closing only after
a quite long time-out.  In some scenarios we need to know very rapidly if a
peer is disconnected or not responding for other reasons (e.g. it is looping).
Since heartbeating can be done at a low level, we implement this as a special
type of frame that peers exchange at the transport level, rather than as a
class method.
*/
type heartbeatFrame struct {
	ChannelId uint16
}

func (f *heartbeatFrame) channel() uint16 { return f.ChannelId }

/*
Certain methods (such as Basic.Publish, Basic.Deliver, etc.) are formally
defined as carrying content.  When a peer sends such a method frame, it always
follows it with a content header and zero or more content body frames.

A content header frame has this format:

	0          2        4           12               14
	+----------+--------+-----------+----------------+------------- - -
	| class-id | weight | body size | property flags | property list...
	+----------+--------+-----------+----------------+------------- - -
	  short     short    long long       short        remainder...

We place content body in distinct frames (rather than including it in the
method) so that AMQP may support "zero copy" techniques in which content is
never marshalled or encoded.  We place the content properties in their own
frame so that recipients can selectively discard contents they do not want to
process
*/
type headerFrame struct {
	ChannelId  uint16
	ClassId    uint16
	weight     uint16
	Size       uint64
	Properties properties
}

func (f *headerFrame) channel() uint16 { return f.ChannelId }

/*
Content is the application data we carry from client-to-client via the AMQP
server.  Content is, roughly speaking, a set of properties plus a binary data
part.  The set of allowed properties are defined by the Basic class, and these
form the "content header frame".  The data can be any size, and MAY be broken
into several (or many) chunks, each forming a "content body frame".

Looking at the frames for a specific channel, as they pass on the wire, we
might see something like this:

	[method]
	[method] [header] [body] [body]
	[method]
	...
*/
type bodyFrame struct {
	ChannelId uint16
	Body      []byte
}

func (f *bodyFrame) channel() uint16 { return f.ChannelId }
