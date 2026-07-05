package mq

// SettlementConsumer processes gift record settlement messages.
// It updates t_gift_record status and notifies the downstream settlement system.
// Business logic lives here directly — no service layer wrapper.
type SettlementConsumer struct {
}

func NewSettlementConsumer() *SettlementConsumer {
	return &SettlementConsumer{}
}

func (c *SettlementConsumer) Start() {
	// TODO: consume from rabbitmq, update record status, handle retry/dead letter
}
