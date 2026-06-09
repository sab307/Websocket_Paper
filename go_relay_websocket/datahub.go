package main

/*
Data Hub  —  WebSocket-only pass-through relay
==============================================

Browser ──WS──► Go (DataHub) ──WS──► Python   (Twist, ClockSyncReq)
Browser ◄──WS── Go (DataHub) ◄──WS── Python   (Ack,   ClockSyncResp)

The hub is a dumb byte forwarder: never decodes, inspects, or rewrites a
frame. Text/binary nature is preserved across both legs. One browser is
paired with one python peer at a time.
*/

import (
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type dataFrame struct {
	data   []byte
	isText bool
}

type dataConn interface {
	writeFrame(dataFrame) error
	closeConn()
	remote() string
}

type DataHub struct {
	mu      sync.RWMutex
	browser dataConn
	python  dataConn
}

var dataHub = &DataHub{}

func (h *DataHub) setPeer(role string, c dataConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if role == "python" {
		if h.python != nil {
			log.Printf("[data] python reconnect — closing old peer")
			h.python.closeConn()
		}
		h.python = c
		log.Printf("[data] + python  %s", c.remote())
	} else {
		if h.browser != nil {
			log.Printf("[data] browser replaced — closing old peer")
			h.browser.closeConn()
		}
		h.browser = c
		log.Printf("[data] + browser %s", c.remote())
	}
}

func (h *DataHub) clearPeer(role string, c dataConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if role == "python" && h.python == c {
		h.python = nil
		log.Printf("[data] - python")
	} else if role == "browser" && h.browser == c {
		h.browser = nil
		log.Printf("[data] - browser")
	}
}

func (h *DataHub) forward(fromRole string, f dataFrame) {
	h.mu.RLock()
	var dst dataConn
	if fromRole == "python" {
		dst = h.browser
	} else {
		dst = h.python
	}
	h.mu.RUnlock()
	if dst == nil {
		return
	}
	if err := dst.writeFrame(f); err != nil {
		log.Printf("[data] forward %s→peer error: %v", fromRole, err)
	}
}

// ─── WebSocket data connection ────────────────────────────────────────────────

type wsDataConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (w *wsDataConn) writeFrame(f dataFrame) error {
	mt := websocket.BinaryMessage
	if f.isText {
		mt = websocket.TextMessage
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteMessage(mt, f.data)
}

func (w *wsDataConn) writePing() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
}

func (w *wsDataConn) closeConn()     { _ = w.conn.Close() }
func (w *wsDataConn) remote() string { return w.conn.RemoteAddr().String() }

func handleData(w http.ResponseWriter, r *http.Request) {
	role := r.URL.Query().Get("role")
	if role != "python" {
		role = "browser"
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[data] upgrade error: %v", err)
		return
	}

	dc := &wsDataConn{conn: conn}
	dataHub.setPeer(role, dc)
	defer func() {
		dataHub.clearPeer(role, dc)
		conn.Close()
	}()

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	stopPing := make(chan struct{})
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-t.C:
				if err := dc.writePing(); err != nil {
					return
				}
			}
		}
	}()
	defer close(stopPing)

	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		dataHub.forward(role, dataFrame{data: data, isText: mt == websocket.TextMessage})
	}
}
