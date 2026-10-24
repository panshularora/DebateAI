package debate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestPollStore(t *testing.T) (*PollStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &PollStore{rdb: rdb, ctx: context.Background()}, mr
}

func TestVoteAcceptsDefinedOption(t *testing.T) {
	ps, mr := newTestPollStore(t)
	pollID, err := ps.CreatePoll("d1", "p1", "Who wins?", []string{"For", "Against"})
	if err != nil {
		t.Fatalf("CreatePoll: %v", err)
	}

	ok, err := ps.Vote("d1", pollID, "For", "spec-1")
	if err != nil || !ok {
		t.Fatalf("Vote(For) = %v, %v; want true, nil", ok, err)
	}
	if got := mr.HGet(fmt.Sprintf("debate:d1:poll:%s:counts", pollID), "For"); got != "1" {
		t.Fatalf("count For = %q; want 1", got)
	}

	// Duplicate vote is rejected without error.
	ok, err = ps.Vote("d1", pollID, "Against", "spec-1")
	if err != nil || ok {
		t.Fatalf("duplicate Vote = %v, %v; want false, nil", ok, err)
	}
}

func TestVoteRejectsUnknownOption(t *testing.T) {
	ps, mr := newTestPollStore(t)
	pollID, err := ps.CreatePoll("d1", "p1", "Who wins?", []string{"For", "Against"})
	if err != nil {
		t.Fatalf("CreatePoll: %v", err)
	}
	countsKey := fmt.Sprintf("debate:d1:poll:%s:counts", pollID)

	for _, opt := range []string{"totally-fake-option-<script>", "", "for"} {
		ok, err := ps.Vote("d1", pollID, opt, "spec-1")
		if ok || !errors.Is(err, ErrInvalidPollOption) {
			t.Fatalf("Vote(%q) = %v, %v; want false, ErrInvalidPollOption", opt, ok, err)
		}
	}

	fields, err := mr.HKeys(countsKey)
	if err != nil {
		t.Fatalf("HKeys: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("counts fields = %v; want only [For Against]", fields)
	}

	// The rejected attempts must not consume the spectator's vote.
	ok, err := ps.Vote("d1", pollID, "Against", "spec-1")
	if err != nil || !ok {
		t.Fatalf("Vote(Against) after invalid attempts = %v, %v; want true, nil", ok, err)
	}
}

func TestVoteRejectsUnknownPoll(t *testing.T) {
	ps, mr := newTestPollStore(t)

	ok, err := ps.Vote("d1", "missing", "For", "spec-1")
	if ok || !errors.Is(err, ErrInvalidPollOption) {
		t.Fatalf("Vote on missing poll = %v, %v; want false, ErrInvalidPollOption", ok, err)
	}
	if mr.Exists("debate:d1:poll:missing:counts") || mr.Exists("debate:d1:poll:missing:voters") {
		t.Fatal("vote on missing poll must not create keys")
	}
}
