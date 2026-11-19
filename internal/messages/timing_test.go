package messages_test

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/messages"
)

func TestTimeHeaderRoundTrip(t *testing.T) {
	at := time.Date(2026, 11, 19, 12, 0, 0, 123456789, time.UTC)
	h := messages.TimeHeader(messages.HeaderOrderTime, at)
	got := messages.HeaderTime([]kgo.RecordHeader{{Key: "other", Value: []byte("x")}, h}, messages.HeaderOrderTime)
	if !got.Equal(at.Truncate(time.Microsecond)) {
		t.Fatalf("got %v, want %v (microsecond precision)", got, at)
	}
}

func TestHeaderTimeMissingOrMalformed(t *testing.T) {
	for name, hs := range map[string][]kgo.RecordHeader{
		"absent":   nil,
		"garbage":  {{Key: messages.HeaderOrderTime, Value: []byte("soon")}},
		"negative": {{Key: messages.HeaderOrderTime, Value: []byte("-5")}},
	} {
		if got := messages.HeaderTime(hs, messages.HeaderOrderTime); !got.IsZero() {
			t.Errorf("%s: got %v, want zero", name, got)
		}
	}
}

func TestWithHeaderReplaces(t *testing.T) {
	in := []kgo.RecordHeader{{Key: "trace", Value: []byte("t")}, {Key: messages.HeaderOrderTime, Value: []byte("1")}}
	out := messages.WithHeader(in, messages.TimeHeader(messages.HeaderOrderTime, time.UnixMicro(2)))
	if len(out) != 2 || out[0].Key != "trace" || string(out[1].Value) != "2" {
		t.Fatalf("headers %+v", out)
	}
	if string(in[1].Value) != "1" {
		t.Fatal("input headers were modified")
	}
}
