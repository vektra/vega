package mailbox

type MessageType int

const (
	NoopType MessageType = iota
	SuccessType
	ErrorType
	DeclareType
	PollType
	PollResultType
	LongPollType
	PushType
)

type Error struct {
	Error string
}

type Declare struct {
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
