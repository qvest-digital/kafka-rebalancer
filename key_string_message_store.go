package rebalance

import (
	"sort"
	"sync"

	"github.com/segmentio/kafka-go"
)

func newKeyStringMessageStore() *keyStringMessageStore {
	return &keyStringMessageStore{
		msgs: make(map[string]timeOrderedMessages),
	}
}

type keyStringMessageStore struct {
	msgs map[string]timeOrderedMessages
	lock sync.RWMutex
}

func (s *keyStringMessageStore) AddMessages(msgs ...kafka.Message) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i := range msgs {
		if s.msgs[string(msgs[i].Key)] == nil {
			s.msgs[string(msgs[i].Key)] = timeOrderedMessages{msgs[i]}
		} else {
			s.msgs[string(msgs[i].Key)] = append(s.msgs[string(msgs[i].Key)], msgs[i])
		}
	}
}

func (s *keyStringMessageStore) Messages() []kafka.Message {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var list []kafka.Message
	for _, msgs := range s.msgs {
		sort.Sort(msgs)
		for i := range msgs {
			msgs[i].Partition = 0
			msgs[i].Offset = 0
			msgs[i].Topic = ""
		}
		list = append(list, msgs...)
	}

	return list
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
