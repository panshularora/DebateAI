package debate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// PollStore handles poll state operations in Redis
type PollStore struct {
	rdb *redis.Client
	ctx context.Context
}

// PollMetadata represents metadata about a poll
type PollMetadata struct {
	PollID   string   `json:"pollId"`
	Question string   `json:"question"`
	Options  []string `json:"options"`
}

// ErrInvalidPollOption is returned when a vote targets an option that is not
// defined on the poll (or the poll does not exist).
var ErrInvalidPollOption = errors.New("invalid poll option")

// voteScript atomically validates the option, records the voter and increments
// the count. HINCRBY would otherwise create arbitrary new options, so the option
// must already exist in the counts hash initialised by CreatePoll.
// Returns 1 on success, 0 on duplicate vote, -1 on invalid option.
var voteScript = redis.NewScript(`
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
	return -1
end
if redis.call("SADD", KEYS[2], ARGV[2]) == 0 then
	return 0
end
redis.call("HINCRBY", KEYS[1], ARGV[1], 1)
return 1
`)

// NewPollStore creates a new PollStore instance
func NewPollStore() *PollStore {
	return &PollStore{
		rdb: GetRedisClient(),
		ctx: GetContext(),
	}
}

// CreatePoll creates a new poll with the provided question and options
func (ps *PollStore) CreatePoll(debateID, pollID, question string, options []string) (string, error) {
	if ps == nil || ps.rdb == nil {
		return "", fmt.Errorf("Redis client not available")
	}

	question = strings.TrimSpace(question)
	if question == "" {
		return "", fmt.Errorf("question is required")
	}

	cleanOptions := make([]string, 0, len(options))
	seen := make(map[string]struct{})
	for _, opt := range options {
		opt = strings.TrimSpace(opt)
		if opt == "" {
			continue
		}
		if _, exists := seen[strings.ToLower(opt)]; exists {
			continue
		}
		seen[strings.ToLower(opt)] = struct{}{}
		cleanOptions = append(cleanOptions, opt)
	}

	if len(cleanOptions) < 2 {
		return "", fmt.Errorf("at least two unique options are required")
	}

	if pollID == "" {
		pollID = uuid.NewString()
	}

	countsKey := fmt.Sprintf("debate:%s:poll:%s:counts", debateID, pollID)
	votersKey := fmt.Sprintf("debate:%s:poll:%s:voters", debateID, pollID)
	metaKey := fmt.Sprintf("debate:%s:poll:%s:meta", debateID, pollID)
	pollsKey := fmt.Sprintf("debate:%s:polls", debateID)

	metadata := PollMetadata{
		PollID:   pollID,
		Question: question,
		Options:  cleanOptions,
	}
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("failed to marshal poll metadata: %w", err)
	}

	pipe := ps.rdb.TxPipeline()

	// Initialize counts to zero for each option
	countFields := make(map[string]interface{}, len(cleanOptions))
	for _, opt := range cleanOptions {
		countFields[opt] = 0
	}
	pipe.Del(ps.ctx, votersKey)
	pipe.HSet(ps.ctx, countsKey, countFields)
	pipe.Set(ps.ctx, metaKey, metaBytes, 0)
	pipe.SAdd(ps.ctx, pollsKey, pollID)

	if _, err := pipe.Exec(ps.ctx); err != nil {
		return "", fmt.Errorf("failed to create poll: %w", err)
	}

	return pollID, nil
}

// Vote handles a vote request and returns whether it was successful
func (ps *PollStore) Vote(debateID, pollID, option, spectatorHash string) (bool, error) {
	if ps == nil || ps.rdb == nil {
		return false, fmt.Errorf("Redis client not available")
	}

	votersKey := fmt.Sprintf("debate:%s:poll:%s:voters", debateID, pollID)
	countsKey := fmt.Sprintf("debate:%s:poll:%s:counts", debateID, pollID)

	res, err := voteScript.Run(ps.ctx, ps.rdb, []string{countsKey, votersKey}, option, spectatorHash).Int64()
	if err != nil {
		return false, fmt.Errorf("failed to record vote: %w", err)
	}

	switch res {
	case 1:
		return true, nil
	case 0:
		// Duplicate vote
		return false, nil
	default:
		return false, ErrInvalidPollOption
	}
}

// GetPollState returns the current poll state for all polls in a debate
func (ps *PollStore) GetPollState(debateID string) (map[string]map[string]int64, map[string]int64, map[string]PollMetadata, error) {
	if ps == nil || ps.rdb == nil {
		return nil, nil, nil, fmt.Errorf("Redis client not available")
	}

	pollIDs := make(map[string]struct{})

	// Gather poll IDs from metadata set
	pollsKey := fmt.Sprintf("debate:%s:polls", debateID)
	pollList, err := ps.rdb.SMembers(ps.ctx, pollsKey).Result()
	if err == nil {
		for _, id := range pollList {
			if id != "" {
				pollIDs[id] = struct{}{}
			}
		}
	}

	// Also gather poll IDs from counts keys (fallback with SCAN)
	pattern := fmt.Sprintf("debate:%s:poll:*:counts", debateID)
	prefix := fmt.Sprintf("debate:%s:poll:", debateID)
	suffix := ":counts"

	var cursor uint64
	for {
		batch, nextCursor, err := ps.rdb.Scan(ps.ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan poll keys: %w", err)
		}
		for _, countsKey := range batch {
			if !strings.HasPrefix(countsKey, prefix) || !strings.HasSuffix(countsKey, suffix) {
				continue
			}
			pollID := countsKey[len(prefix) : len(countsKey)-len(suffix)]
			if pollID != "" {
				pollIDs[pollID] = struct{}{}
			}
		}
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	pollState := make(map[string]map[string]int64)
	votersCount := make(map[string]int64)
	metadataMap := make(map[string]PollMetadata)

	for pollID := range pollIDs {
		countsKey := fmt.Sprintf("debate:%s:poll:%s:counts", debateID, pollID)
		metaKey := fmt.Sprintf("debate:%s:poll:%s:meta", debateID, pollID)
		votersKey := fmt.Sprintf("debate:%s:poll:%s:voters", debateID, pollID)

		// Get counts
		counts, err := ps.rdb.HGetAll(ps.ctx, countsKey).Result()
		if err == nil && len(counts) > 0 {
			pollState[pollID] = make(map[string]int64)
			for option, countStr := range counts {
				var count int64
				if _, err := fmt.Sscanf(countStr, "%d", &count); err == nil {
					pollState[pollID][option] = count
				}
			}
		} else {
			pollState[pollID] = make(map[string]int64)
		}

		// Get voter count
		voterCount, _ := ps.rdb.SCard(ps.ctx, votersKey).Result()
		votersCount[pollID] = voterCount

		// Get metadata
		metaStr, err := ps.rdb.Get(ps.ctx, metaKey).Result()
		if err == nil && metaStr != "" {
			var meta PollMetadata
			if err := json.Unmarshal([]byte(metaStr), &meta); err == nil {
				if len(meta.Options) == 0 {
					// fallback to counts keys if options missing
					if counts != nil {
						for option := range counts {
							meta.Options = append(meta.Options, option)
						}
					}
				}
				if meta.PollID == "" {
					meta.PollID = pollID
				}
				metadataMap[pollID] = meta
				continue
			}
		}

		// Fallback metadata if stored value missing/invalid
		options := make([]string, 0, len(pollState[pollID]))
		for option := range pollState[pollID] {
			options = append(options, option)
		}
		metadataMap[pollID] = PollMetadata{
			PollID:   pollID,
			Question: "",
			Options:  options,
		}
	}

	return pollState, votersCount, metadataMap, nil
}

// HasVoted checks if a spectator has already voted
func (ps *PollStore) HasVoted(debateID, pollID, spectatorHash string) (bool, error) {
	if ps == nil || ps.rdb == nil {
		return false, fmt.Errorf("Redis client not available")
	}

	votersKey := fmt.Sprintf("debate:%s:poll:%s:voters", debateID, pollID)
	exists, err := ps.rdb.SIsMember(ps.ctx, votersKey, spectatorHash).Result()
	if err != nil {
		return false, err
	}
	return exists, nil
}
