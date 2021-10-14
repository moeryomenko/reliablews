package websocket

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

var upgrader = websocket.Upgrader{}

type stubBehavior func(*testing.T, context.Context, *websocket.Conn)

func TestWebsocket(t *testing.T) {
	mockServer := func(t *testing.T, ctx context.Context, callCounter *int32, stub stubBehavior) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			c, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			atomic.AddInt32(callCounter, 1)
			go stub(t, ctx, c)
		}
	}

	testcases := map[string]struct {
		stub     stubBehavior
		counter  int32
		options  []Option
		expected []byte
	}{
		`pong missed timeout, will reconnect`: {
			expected: []byte("test"),
			options: []Option{
				WithPingPeriod(50 * time.Millisecond),
				WithPongWait(20 * time.Millisecond),
				WithReadPeriod(100 * time.Millisecond),
			},
			stub: func(t *testing.T, ctx context.Context, c *websocket.Conn) {
				defer c.Close()

				skipFirst := int32(0)
				c.SetPingHandler(func(appData string) error {
					if skipFirst < 2 {
						atomic.AddInt32(&skipFirst, 1)
						return nil
					}
					err := c.WriteControl(websocket.PongMessage, []byte{}, time.Now().Add(time.Second))
					if err == websocket.ErrCloseSent {
						return nil
					} else if e, ok := err.(net.Error); ok && e.Temporary() {
						return nil
					}
					return err
				})

				for {
					select {
					case <-ctx.Done():
						return
					default:
						c.WriteMessage(websocket.TextMessage, []byte("test"))
					}
				}
			},
		},
	}

	for name, testcase := range testcases {
		testcase := testcase

		t.Run(name, func(t *testing.T) {
			gracefulCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
			defer cancel()

			callCounter := int32(0)
			s := httptest.NewServer(mockServer(t, gracefulCtx, &callCounter, testcase.stub))
			defer s.Close()

			url := "ws" + strings.TrimPrefix(s.URL, "http")
			wsconnect := NewWebSocket(gracefulCtx, url, testcase.options...)
			defer wsconnect.Close()
			msg, err := wsconnect.ReadMessage(gracefulCtx)
			// in order not to happen, we must receive data.
			assert.NoError(t, err)
			assert.Equal(t, testcase.expected, msg)
			assert.NotEqual(t, int32(1), callCounter)
		})
	}
}
