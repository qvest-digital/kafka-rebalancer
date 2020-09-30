package rebalance

import (
	"sort"
	"sync"

	"github.com/segmentio/kafka-go"
)

func newMessageStore() *MessageStore {
	return &MessageStore{
		msgs: make(timeOrderedMessages, 0),
	}
}

type MessageStore struct {
	msgs timeOrderedMessages
	lock sync.RWMutex
}

func (s *MessageStore) AddMessages(msgs ...kafka.Message) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.msgs = append(s.msgs, msgs...)
}

func (s *MessageStore) Messages() []kafka.Message {
	s.lock.RLock()
	defer s.lock.RUnlock()

	sort.Sort(s.msgs)
	for i := range s.msgs {
		s.msgs[i] = cleanMessage(s.msgs[i])
	}

	return []kafka.Message(s.msgs)
}

type timeOrderedMessages []kafka.Message

func (s timeOrderedMessages) Len() int {
	return len(s)
}

func (s timeOrderedMessages) Less(i, j int) bool {
	return s[i].Time.Before(s[j].Time)
}

func (s timeOrderedMessages) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func messagesToKeyStrings(msgs []kafka.Message) []string {
	keys := make([]string, len(msgs))
	for i := range keys {
		keys[i] = string(msgs[i].Key)
	}
	return keys
}
