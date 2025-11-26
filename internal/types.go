package internal

type LogMessage struct {
	ID string `json:"id"`
	Timestamp int64 `json:"timestamp"`
	Level string `json:"level"`
	Service string `json:"service"`
	Message string `json:"message"`
	ProcessedAt string `json:"processed_at,omitempty"`
}