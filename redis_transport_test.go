package mercure

import (
	"net/url"
	"testing"
	"github.com/go-redis/redismock/v8"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"encoding/json"
	"sync"
	"time"
	"fmt"
)

func createRedisTransport(t *testing.T, dsn string, debug bool) (*RedisTransport, redismock.ClientMock) {
	u, err := url.Parse(dsn)
	assert.Nil(t, err)
	var logger Logger
	if debug {
		logger, _ = zap.NewDevelopment()
	} else {
		logger = zap.NewNop()
	}
	rt, err := makeRedisTransport(u, logger)
	assert.Nil(t, err)
	client, mock := redismock.NewClientMock()
	rt.client = client
	return rt, mock
}

func TestRedisTransportHistory(t *testing.T) {
	transport, mock := createRedisTransport(t, "redis://127.0.0.1", false)
	defer transport.Close()

	topics := []string{"https://example.com/foo"}
	upd := make([]Update, 10)
	for i := 0; i < 10; i++ {
		upd[i] = Update{
			Event:  Event{ID: strconv.Itoa(i+1)},
			Topics: topics,
		}
	}

	updjs := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		var err error
		updjs[i], err = json.Marshal(upd[i])
		assert.Nil(t, err)
		mock.ExpectXAdd(&redis.XAddArgs{
			Stream: `mercure`,
			ID: `*`,
			Values: map[string]interface{}{"update": updjs[i]},
		}).SetVal(strconv.Itoa(i+1))
		mock.ExpectSet(strconv.Itoa(i+1), strconv.Itoa(i+1), 0).SetVal("OK")
	}

	for _, u := range upd {
		transport.Dispatch(&u)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

	s := NewSubscriber("8", transport.logger)
	s.SetTopics(topics, nil)

	mock.ExpectGet("8").SetVal("8")
	mock.ExpectXReadStreams("mercure", "8").SetVal([]redis.XStream{
		{ Stream: "mercure",
		  Messages: []redis.XMessage{
			  { ID: "9", Values: map[string]interface{}{"update": string(updjs[8])} },
			  { ID: "10", Values: map[string]interface{}{"update": string(updjs[9])} },
		  },
		},
	})
	require.Nil(t, transport.AddSubscriber(s))

	assert.Equal(t, "9", (<-s.Receive()).ID)
	assert.Equal(t, "10", (<-s.Receive()).ID)
	assert.Equal(t, "10", (<-s.responseLastEventID))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRedisTransportRetrieveAllHistory(t *testing.T) {
	transport, mock := createRedisTransport(t, "redis://127.0.0.1", false)
	defer transport.Close()

	topics := []string{"https://example.com/foo"}
	upd := make([]Update, 10)
	for i := 0; i < 10; i++ {
		upd[i] = Update{
			Event:  Event{ID: strconv.Itoa(i+1)},
			Topics: topics,
		}
	}

	updjs := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		var err error
		updjs[i], err = json.Marshal(upd[i])
		assert.Nil(t, err)
		mock.ExpectXAdd(&redis.XAddArgs{
			Stream: `mercure`,
			ID: `*`,
			Values: map[string]interface{}{"update": updjs[i]},
		}).SetVal(strconv.Itoa(i+1))
		mock.ExpectSet(strconv.Itoa(i+1), strconv.Itoa(i+1), 0).SetVal("OK")
	}

	for _, u := range upd {
		transport.Dispatch(&u)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

	s := NewSubscriber(EarliestLastEventID, transport.logger)
	s.SetTopics(topics, nil)

	msg := make([]redis.XMessage, 10)
	for i, u := range updjs {
		msg[i] = redis.XMessage{ ID: strconv.Itoa(i+1), Values: map[string]interface{}{"update": string(u) } }
	}

	mock.ExpectGet(EarliestLastEventID).RedisNil()
	mock.ExpectXReadStreams("mercure", "0-0").SetVal([]redis.XStream{
		{ Stream: "mercure", Messages: msg },
	})
	require.Nil(t, transport.AddSubscriber(s))

	for _, u := range upd {
		assert.Equal(t, u.ID, (<-s.Receive()).ID)
	}

	assert.Equal(t, "10", (<-s.responseLastEventID))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRedisTopicSelectorHistory(t *testing.T) {
	transport, mock := createRedisTransport(t, "redis://127.0.0.1", false)
	defer transport.Close()

	upd := []Update{
		{Topics: []string{"http://example.com/subscribed"}, Event: Event{ID: "1"}},
		{Topics: []string{"http://example.com/not-subscribed"}, Event: Event{ID: "2"}},
		{Topics: []string{"http://example.com/subscribed-public-only"}, Private: true, Event: Event{ID: "3"}},
		{Topics: []string{"http://example.com/subscribed-public-only"}, Event: Event{ID: "4"}},
	}

	var updjs []string
	for _, u := range upd {
		var err error
		js, err := json.Marshal(u)
		assert.Nil(t, err)
		updjs = append(updjs, string(js))
	}
	
	s := NewSubscriber(EarliestLastEventID, transport.logger)
	s.SetTopics([]string{"http://example.com/subscribed", "http://example.com/subscribed-public-only"}, []string{"http://example.com/subscribed"})

	var msg []redis.XMessage
	for i, u := range updjs {
		msg = append(msg, redis.XMessage{ ID: strconv.Itoa(i+1), Values: map[string]interface{}{"update": u } })
	}
	
	mock.ExpectGet(EarliestLastEventID).RedisNil()
	mock.ExpectXReadStreams("mercure", "0-0").SetVal([]redis.XStream{
		{ Stream: "mercure", Messages: msg },
	})
	
	require.Nil(t, transport.AddSubscriber(s))

	assert.Equal(t, "1", (<-s.Receive()).ID)
	assert.Equal(t, "4", (<-s.Receive()).ID)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRedisTransportHistoryAndLive(t *testing.T) {
	transport, mock := createRedisTransport(t, "redis://127.0.0.1", false)
	defer transport.Close()
	topics := []string{"https://example.com/foo"}

	upd := make([]Update, 11)
	updjs := make([][]byte, 11)
	for i := 0; i < 11; i++ {
		upd[i] = Update{
			Event:  Event{ID: strconv.Itoa(i+1)},
			Topics: topics,
		}
		var err error
		updjs[i], err = json.Marshal(upd[i])
		assert.Nil(t, err)
	}

	var msg []redis.XMessage
	for i, u := range updjs {
		msg = append(msg, redis.XMessage{ ID: strconv.Itoa(i+1), Values: map[string]interface{}{"update": u } })
	}

	s := NewSubscriber("8", transport.logger)
	s.SetTopics(topics, nil)

	mock.ExpectGet("8").SetVal("8")
	mock.ExpectXReadStreams("mercure", "8").SetVal([]redis.XStream{
		{ Stream: "mercure",
		  Messages: []redis.XMessage{
			  { ID: "9", Values: map[string]interface{}{"update": string(updjs[8])} },
			  { ID: "10", Values: map[string]interface{}{"update": string(updjs[9])} },
		  },
		},
	})
	require.Nil(t, transport.AddSubscriber(s))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.Equal(t, "9", (<-s.Receive()).ID)
		assert.Equal(t, "10", (<-s.Receive()).ID)
		assert.Equal(t, "11", (<-s.Receive()).ID)
	}()

	mock.ExpectXAdd(&redis.XAddArgs{
		Stream: `mercure`,
		ID: `*`,
		Values: map[string]interface{}{"update": updjs[10]},
	}).SetVal("11")
	mock.ExpectSet("11", "11", 0).SetVal("OK")

	transport.Dispatch(&upd[10])

	wg.Wait()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRedisTransportPurgeHistory(t *testing.T) {
	transport, mock := createRedisTransport(t, "redis://127.0.0.1?event_ttl=20s", false)
	defer transport.Close()

	topics := []string{"https://example.com/foo"}

	ids := []int{
		10000,
		14000,
		20000,
		25000,
		92000,
		100000,
		110000,
	}

	var msg []redis.XMessage
	for i, id := range ids {
		upd := Update{
			Event:  Event{ID: strconv.Itoa(i+1)},
			Topics: topics,
		}
		ujs, err := json.Marshal(upd)
		assert.Nil(t, err)
		msg = append(msg, redis.XMessage{ ID: strconv.Itoa(id), Values: map[string]interface{}{"update": string(ujs) } })
	}
	
	mock.ExpectTime().SetVal(time.Unix(111, 0))
	mock.ExpectXRange("mercure", "0", "91000").SetVal(msg[0:4])
	mock.ExpectDel("1", "2", "3", "4").SetVal(4)
	// As of tag v8.0.6, redismock lacks ExpectXTrimMinID method.  Use CustomMatch to compensate
	// for that.
	mock.CustomMatch(func (exp, act []interface{}) error {
		expected := []string{"xtrim", "mercure", "minid", "91000"}
		if len(act) != len(expected) {
			return fmt.Errorf("expected length mismatch; got %d, expected %d", len(act), len(expected))
		}
		
		for i, v := range act {
			if s, ok := v.(string); ok {
				if s != expected[i] {
					fmt.Errorf("term %d mismatch: got %s, expected %s", i, s, expected[i])
				}
			} else {
				fmt.Errorf("term %d type mismatch", i)
			}
		}
		return nil
	}).ExpectXTrim("mercure", 0).SetVal(4)	
	transport.trim()
}
