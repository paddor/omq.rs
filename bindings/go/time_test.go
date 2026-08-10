package omq

import (
	"testing"
	"time"
)

func TestDurationMillisRoundsPositiveDurationsUp(t *testing.T) {
	tests := []struct {
		duration time.Duration
		want     int64
	}{
		{duration: 0, want: 0},
		{duration: time.Nanosecond, want: 1},
		{duration: time.Millisecond, want: 1},
		{duration: time.Millisecond + time.Nanosecond, want: 2},
		{duration: -time.Nanosecond, want: -1},
	}
	for _, test := range tests {
		if got := durationMillis(test.duration); got != test.want {
			t.Fatalf("durationMillis(%s) = %d, want %d", test.duration, got, test.want)
		}
	}
}
