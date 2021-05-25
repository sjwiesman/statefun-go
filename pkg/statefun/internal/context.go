package internal

type ContextKey string

const (
	SelfKey    = ContextKey("self")
	CallerKey  = ContextKey("caller")
	MailboxKey = ContextKey("mailbox")
)
