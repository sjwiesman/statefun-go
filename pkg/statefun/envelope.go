package statefun

// Implemented by any type that can be put into the mailbox
type Envelope interface {
	isEnvelope()
}
