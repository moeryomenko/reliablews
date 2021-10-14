package websocket

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

const (
	// Wait time for message sends to succeed.
	writeTimeout = 10 * time.Second
	// How often reading message from connection.
	readPeriod = time.Second
	// Time allowed to read the next pong message from the peer.
	pongWait = 30 * time.Second
	// Send pings to peer with this period. Must be less then pongWait.
	pingPeriod = (pongWait * 9) / 10
)

// WebSocket respresents autoreconnected websocket connection upon gorilla/websocket.
type WebSocket struct {
	url string
	mu  sync.RWMutex

	conn       net.Conn
	sendBuffer chan []byte
	recvBuffer chan []byte
	closec     chan error

	cleanupHook   func(context.Context)
	reconnectHook func(context.Context, *WebSocket) error
	ctx           context.Context
	cancel        func()

	pongRecv     chan struct{}
	pongWait     time.Duration
	pingPeriod   time.Duration
	readPeriod   time.Duration
	writeTimeout time.Duration
}

// Option is an option that can be applied to WebSocket.
type Option func(*WebSocket)

// WithReconnectHook sets a hook that will run after reconnect.
func WithReconnectHook(hook func(context.Context, *WebSocket) error) Option {
	return func(ws *WebSocket) {
		ws.reconnectHook = hook
	}
}

// WithWriteTimeout sets timeout for write.
func WithWriteTimeout(writeTimeout time.Duration) Option {
	return func(wsocket *WebSocket) {
		wsocket.writeTimeout = writeTimeout
	}
}

// WithReadPeriod sets read period.
func WithReadPeriod(readPeriod time.Duration) Option {
	return func(wsocket *WebSocket) {
		wsocket.readPeriod = readPeriod
	}
}

// WithPingPeriod sets ping period.
func WithPingPeriod(pingPeriod time.Duration) Option {
	return func(wsocket *WebSocket) {
		wsocket.pingPeriod = pingPeriod
	}
}

// WithPongWait sets pong wait.
func WithPongWait(pongWait time.Duration) Option {
	return func(wsocket *WebSocket) {
		wsocket.pongWait = pongWait
	}
}

// WithCleanupHook sets a hook that will run after close websocket.
func WithCleanupHook(hook func(context.Context)) Option {
	return func(wsocket *WebSocket) {
		wsocket.cleanupHook = hook
	}
}

// NewWebSocket returns new reliable websocket connection.
func NewWebSocket(ctx context.Context, wsocketURL string, opts ...Option) *WebSocket {
	wsocket := &WebSocket{
		url:          wsocketURL,
		writeTimeout: writeTimeout,
		pingPeriod:   pingPeriod,
		pongWait:     pongWait,
		readPeriod:   readPeriod,
		recvBuffer:   make(chan []byte),
		sendBuffer:   make(chan []byte),
		pongRecv:     make(chan struct{}),
	}

	// need for gracefully shutdown read/write goroutines.
	wsocket.ctx, wsocket.cancel = context.WithCancel(ctx)

	for _, opt := range opts {
		opt(wsocket)
	}

	// There's a number of operations happening in parallel per websocket connection
	// - periodic sending PINGs, for control health of connection;
	// - writing messages
	// - receiving messages
	// When writing or reading over the websocket fails, re-establish the connection next time.
	go wsocket.heartbeat()
	go wsocket.writePump()
	go wsocket.readPump()

	return wsocket
}

// Close closes connection.
func (wsocket *WebSocket) Close() {
	wsocket.writeClose()
	wsocket.cleanup()
	wsocket.cancel()
}

// ReadMessage reads message from connection.
func (wsocket *WebSocket) ReadMessage(ctx context.Context) ([]byte, error) {
	for {
		select {
		case msg := <-wsocket.recvBuffer:
			return msg, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// WriteMessage writes message to connection.
func (wsocket *WebSocket) WriteMessage(ctx context.Context, msg []byte) error {
	for {
		select {
		case wsocket.sendBuffer <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// NotifyError notifies the connection about error and re-establish the connection.
func (wsocket *WebSocket) NotifyError(err error) {
	wsocket.closec <- err
}

// Runs periodic reading of messages from the connection.
func (wsocket *WebSocket) readPump() {
	// TODO(moeryomenko): remove periodic reading.
	// investigate possible solutions to the problem https://github.com/golang/go/issues/15735.
	ticker := time.NewTicker(wsocket.readPeriod)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			err := wsocket.establish()
			if err != nil {
				return
			}

			message, err := wsutil.ReadServerText(wsocket.conn)
			if err != nil {
				wsocket.closec <- err
				continue
			}

			wsocket.recvBuffer <- message
		case <-wsocket.ctx.Done():
			return
		}
	}
}

// Runs writer worker.
func (wsocket *WebSocket) writePump() {
	for {
		select {
		case message := <-wsocket.sendBuffer:
			err := wsocket.establish()
			if err != nil {
				return
			}

			err = wsocket.conn.SetWriteDeadline(time.Now().Add(wsocket.writeTimeout))
			if err != nil {
				wsocket.closec <- err
				continue
			}

			err = wsutil.WriteClientText(wsocket.conn, message)
			if err != nil && !errors.Is(err, net.ErrClosed) {
				wsocket.closec <- err
				continue
			}
		case <-wsocket.ctx.Done():
			return
		}
	}
}

func (wsocket *WebSocket) cleanup() {
	wsocket.mu.Lock()
	defer wsocket.mu.Unlock()
	close(wsocket.closec)
	close(wsocket.sendBuffer)
	close(wsocket.recvBuffer)

	wsocket.conn.Close()

	if wsocket.cleanupHook != nil {
		wsocket.cleanupHook(wsocket.ctx)
	}
}

func (wsocket *WebSocket) writeClose() {
	err := wsocket.conn.SetWriteDeadline(time.Now().Add(wsocket.writeTimeout))
	if err != nil {
		return
	}
	// nolint: errcheck // it's don't matter on close connection.
	wsutil.WriteClientMessage(wsocket.conn, ws.OpClose, nil)
}

// Runs a periodic check for the health of the connection.
func (wsocket *WebSocket) heartbeat() {
	ticker := time.NewTicker(wsocket.pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := wsocket.establish()
			if err != nil {
				return
			}

			deadline := time.Now().Add(wsocket.pingPeriod / 2)
			err = wsocket.conn.SetWriteDeadline(deadline)
			if err != nil {
				wsocket.closec <- err
				continue
			}

			err = wsutil.WriteClientMessage(wsocket.conn, ws.OpPing, nil)
			if err != nil && !errors.Is(err, net.ErrClosed) {
				wsocket.closec <- err // re-esteblish connection next time.
				continue
			}

			err = wsocket.conn.SetReadDeadline(time.Now().Add(wsocket.pongWait))
			if err != nil {
				wsocket.closec <- err
				continue
			}
			err = wsutil.HandleServerControlMessage(wsocket.conn, wsutil.Message{OpCode: ws.OpPong})
			if err != nil && !errors.Is(err, net.ErrClosed) {
				wsocket.closec <- err
				continue
			}
		case <-wsocket.ctx.Done():
			return
		}
	}
}

// establish checks connection status, and returns either the current connection
// or establish a new connection.
func (wsocket *WebSocket) establish() error {
	// in fast path we can just check closec.
	wsocket.mu.RLock()
	if wsocket.conn != nil {
		select {
		// If it was closed, open a new one.
		case <-wsocket.closec:
			// If it isn't closed, nothing to do.
		default:
			wsocket.mu.RUnlock()
			return nil
		}
	}

	// slow path: releae read lock, and acquire write lock and reconnect.
	wsocket.mu.RUnlock()
	wsocket.mu.Lock()
	defer wsocket.mu.Unlock()
	// Create a new connection.
	var conn net.Conn
	err := runWithContext(wsocket.ctx, func() (err error) {
		// TODO(moeryomenko): maybe move custom dialer to WebSocket struct.
		conn, _, _, err = ws.Dialer{Timeout: 5 * time.Second}.Dial(wsocket.ctx, wsocket.url)
		return err
	})
	if err != nil {
		return err
	}
	wsocket.conn = conn
	wsocket.closec = make(chan error, 1) // closec will get at most one element.
	if wsocket.reconnectHook != nil {
		// make additional things (like subscribe), before work with new connection.
		return wsocket.reconnectHook(wsocket.ctx, wsocket)
	}

	return nil
}

// Run f while checking to see if ctx is done.
// Return the error from f if it completes, or ctx.Err() if ctx is done.
func runWithContext(ctx context.Context, f func() error) error {
	c := make(chan error, 1) // buffer so the goroutine can finish even if ctx is done
	go func() { c <- f() }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c:
		return err
	}
}
