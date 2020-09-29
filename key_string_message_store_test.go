package rebalance

import (
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestKeyStringMessageStore(t *testing.T) {
	tests := []struct {
		name             string
		inputMessages    []kafka.Message
		expectedMessages []kafka.Message
	}{
		{
			name: "Messages from one partition in right order",
			inputMessages: []kafka.Message{
				{
					Partition: 1,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 1,
					Offset:    1,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
			},
			expectedMessages: []kafka.Message{
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
			},
		},
		{
			name: "Messages from one partition in wrong order timewise",
			inputMessages: []kafka.Message{
				{
					Partition: 1,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 1,
					Offset:    1,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
			},
			expectedMessages: []kafka.Message{
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
			},
		},
		{
			name: "Messages from one partition in wrong order offsetwise",
			inputMessages: []kafka.Message{
				{
					Partition: 1,
					Offset:    1,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 1,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
			},
			expectedMessages: []kafka.Message{
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
			},
		},
		{
			name: "Messages from two partitions",
			inputMessages: []kafka.Message{
				{
					Partition: 1,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 2,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 1,
					Offset:    1,
					Key:       []byte("foo"),
					Value:     []byte("foo-3"),
					Time:      time.Date(2020, 9, 15, 10, 5, 30, 0, time.UTC),
				},
			},
			expectedMessages: []kafka.Message{
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-1"),
					Time:      time.Date(2020, 9, 13, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-2"),
					Time:      time.Date(2020, 9, 14, 10, 5, 30, 0, time.UTC),
				},
				{
					Partition: 0,
					Offset:    0,
					Key:       []byte("foo"),
					Value:     []byte("foo-3"),
					Time:      time.Date(2020, 9, 15, 10, 5, 30, 0, time.UTC),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := keyStringMessageStore{
				msgs: make(map[string]timeOrderedMessages),
			}
			store.AddMessages(tt.inputMessages...)
			act := store.Messages()

			// first without fields for better readability
			actKeys := messagesToKeyStrings(act)
			expKeys := messagesToKeyStrings(tt.expectedMessages)
			if !reflect.DeepEqual(actKeys, expKeys) {
				t.Errorf(
					"Unexpected messages\nactual:\t%#v\nexpect:\t%#v",
					actKeys,
					expKeys,
				)
			}
			// now with fields
			if !reflect.DeepEqual(act, tt.expectedMessages) {
				t.Errorf(
					"Unexpected message fields (order is right)\nactual:\t%#v\nexpect:\t%#v",
					act,
					tt.expectedMessages,
				)
			}
		})
	}
}
