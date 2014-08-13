package vega

type MessageType int

const (
	NoopType MessageType = iota
	SuccessType
	ErrorType
	DeclareType
	EphemeralDeclareType
	AbandonType
	PollType
	PollResultType
	LongPollType
	PushType
	CloseType
	NackType
	AckType
	StatsType
	StatsResultType
)

type Error struct {
	Error string
}

type Declare struct {
	Name string
}

type Abandon struct {
	Name string
}

type Poll struct {
	Name string
}

type LongPoll struct {
	Name     string
	Duration string
}

type PollResult struct {
	Message *Message
}

type Push struct {
	Name    string
	Message *Message
}

type NackMessage struct {
	MessageId MessageId
}

type AckMessage struct {
	MessageId MessageId
}

type ClientStats struct {
	InFlight int
}
