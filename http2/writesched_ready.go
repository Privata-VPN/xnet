// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !(go1.27 && !http2legacy)

package http2

import (
	"fmt"
	"math"
)

// readyRoundRobinWriteScheduler is a round-robin scheduler like
// roundRobinWriteScheduler, except that its ring holds only streams with
// queued frames. Pop then costs O(1) instead of O(open streams), which
// matters to servers holding many mostly idle streams per connection.
type readyRoundRobinWriteScheduler struct {
	// control contains control frames (SETTINGS, PING, etc.).
	control writeQueue

	// streams maps stream ID to a queue.
	streams map[uint32]*writeQueue

	// head is the next queue to write in the ring of non-empty queues,
	// or nil if no stream has a queued frame.
	head *writeQueue

	// pool of empty queues for reuse.
	queuePool writeQueuePool
}

// NewReadyRoundRobinWriteScheduler constructs a write scheduler that sends
// control frames first and then rotates over the streams that have frames
// queued, skipping idle streams at no cost.
func NewReadyRoundRobinWriteScheduler() WriteScheduler {
	return &readyRoundRobinWriteScheduler{
		streams: make(map[uint32]*writeQueue),
	}
}

func (ws *readyRoundRobinWriteScheduler) OpenStream(streamID uint32, options OpenStreamOptions) {
	if ws.streams[streamID] != nil {
		panic(fmt.Errorf("stream %d already opened", streamID))
	}
	ws.streams[streamID] = ws.queuePool.get()
}

func (ws *readyRoundRobinWriteScheduler) CloseStream(streamID uint32) {
	q := ws.streams[streamID]
	if q == nil {
		return
	}
	ws.unlink(q)
	delete(ws.streams, streamID)
	ws.queuePool.put(q)
}

func (ws *readyRoundRobinWriteScheduler) AdjustStream(streamID uint32, priority PriorityParam) {}

func (ws *readyRoundRobinWriteScheduler) Push(wr FrameWriteRequest) {
	if wr.isControl() {
		ws.control.push(wr)
		return
	}
	q := ws.streams[wr.StreamID()]
	if q == nil {
		// This is a closed stream.
		// wr should not be a HEADERS or DATA frame.
		// We push the request onto the control queue.
		if wr.DataSize() > 0 {
			panic("add DATA on non-open stream")
		}
		ws.control.push(wr)
		return
	}
	q.push(wr)
	if q.next == nil {
		ws.link(q)
	}
}

func (ws *readyRoundRobinWriteScheduler) Pop() (FrameWriteRequest, bool) {
	// Control and RST_STREAM frames first.
	if !ws.control.empty() {
		return ws.control.shift(), true
	}
	q := ws.head
	if q == nil {
		return FrameWriteRequest{}, false
	}
	for {
		// A queue stays in the ring while its first frame waits for flow
		// control, so only streams with frames ready to go are skipped over.
		if wr, ok := q.consume(math.MaxInt32); ok {
			next := q.next
			if q.empty() {
				ws.unlink(q)
			}
			if ws.head != nil {
				ws.head = next
			}
			return wr, true
		}
		q = q.next
		if q == ws.head {
			return FrameWriteRequest{}, false
		}
	}
}

// link inserts q at the end of the ring, just before head.
func (ws *readyRoundRobinWriteScheduler) link(q *writeQueue) {
	if ws.head == nil {
		ws.head = q
		q.next = q
		q.prev = q
		return
	}
	q.prev = ws.head.prev
	q.next = ws.head
	q.prev.next = q
	q.next.prev = q
}

// unlink removes q from the ring if it is in it.
func (ws *readyRoundRobinWriteScheduler) unlink(q *writeQueue) {
	if q.next == nil {
		return
	}
	if q.next == q {
		ws.head = nil
	} else {
		q.prev.next = q.next
		q.next.prev = q.prev
		if ws.head == q {
			ws.head = q.next
		}
	}
	q.next = nil
	q.prev = nil
}
