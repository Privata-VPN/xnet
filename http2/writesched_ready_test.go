// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !(go1.27 && !http2legacy)

package http2

import (
	"reflect"
	"testing"
)

func newReadyTestStream(sc *serverConn, ws WriteScheduler, id uint32, window int32) *stream {
	st := &stream{id: id, sc: sc}
	st.flow.add(window)
	ws.OpenStream(id, OpenStreamOptions{})
	return st
}

func pushReadyTestData(ws WriteScheduler, st *stream, n int) {
	ws.Push(FrameWriteRequest{
		write:  &writeData{streamID: st.id, p: make([]byte, n)},
		stream: st,
	})
}

func popReadyTestStreams(t *testing.T, ws WriteScheduler, frameSize int) []uint32 {
	t.Helper()
	var got []uint32
	for {
		wr, ok := ws.Pop()
		if !ok {
			return got
		}
		if wr.DataSize() != frameSize {
			t.Fatalf("wr.Pop() = %v data bytes, want %v", wr.DataSize(), frameSize)
		}
		got = append(got, wr.StreamID())
	}
}

func TestReadyRoundRobinScheduler(t *testing.T) {
	const maxFrameSize = 16
	sc := &serverConn{maxFrameSize: maxFrameSize}
	ws := NewReadyRoundRobinWriteScheduler()
	for i := 0; i < 4; i++ {
		st := newReadyTestStream(sc, ws, uint32(i)+1, 1<<20)
		pushReadyTestData(ws, st, maxFrameSize*(i+1))
	}
	const controlFrames = 2
	for i := 0; i < controlFrames; i++ {
		ws.Push(makeWriteNonStreamRequest())
	}
	for i := 0; i < controlFrames; i++ {
		wr, ok := ws.Pop()
		if !ok || wr.StreamID() != 0 {
			t.Fatalf("wr.Pop() = stream %v, %v; want 0, true", wr.StreamID(), ok)
		}
	}
	want := []uint32{1, 2, 3, 4, 2, 3, 4, 3, 4, 4}
	if got := popReadyTestStreams(t, ws, maxFrameSize); !reflect.DeepEqual(got, want) {
		t.Fatalf("popped streams %v, want %v", got, want)
	}
}

func TestReadyRoundRobinSchedulerRelinksAndCloses(t *testing.T) {
	const maxFrameSize = 16
	sc := &serverConn{maxFrameSize: maxFrameSize}
	ws := NewReadyRoundRobinWriteScheduler()
	streams := make([]*stream, 3)
	for i := range streams {
		streams[i] = newReadyTestStream(sc, ws, uint32(i)+1, 1<<20)
	}
	if _, ok := ws.Pop(); ok {
		t.Fatal("Pop with only idle streams returned a frame")
	}

	// A stream that drained and gets data again rejoins the ring.
	pushReadyTestData(ws, streams[0], maxFrameSize)
	if got := popReadyTestStreams(t, ws, maxFrameSize); !reflect.DeepEqual(got, []uint32{1}) {
		t.Fatalf("popped streams %v, want [1]", got)
	}
	pushReadyTestData(ws, streams[2], maxFrameSize)
	pushReadyTestData(ws, streams[0], 2*maxFrameSize)
	if got := popReadyTestStreams(t, ws, maxFrameSize); !reflect.DeepEqual(got, []uint32{3, 1, 1}) {
		t.Fatalf("popped streams %v, want [3 1 1]", got)
	}

	// Closing a stream with queued frames drops it from the ring.
	pushReadyTestData(ws, streams[1], maxFrameSize)
	pushReadyTestData(ws, streams[2], maxFrameSize)
	ws.CloseStream(2)
	if got := popReadyTestStreams(t, ws, maxFrameSize); !reflect.DeepEqual(got, []uint32{3}) {
		t.Fatalf("popped streams %v, want [3]", got)
	}

	// A reopened ID starts out of the ring.
	streams[1] = newReadyTestStream(sc, ws, 2, 1<<20)
	if _, ok := ws.Pop(); ok {
		t.Fatal("Pop after reopening an idle stream returned a frame")
	}
}

func TestReadyRoundRobinSchedulerFlowControl(t *testing.T) {
	const maxFrameSize = 16
	sc := &serverConn{maxFrameSize: maxFrameSize}
	sc.flow.add(1 << 20)
	ws := NewReadyRoundRobinWriteScheduler()
	blocked := newReadyTestStream(sc, ws, 1, 0)
	ready := newReadyTestStream(sc, ws, 3, 1<<20)
	pushReadyTestData(ws, blocked, maxFrameSize)
	pushReadyTestData(ws, ready, maxFrameSize)

	// A stream without window is skipped but keeps its place in the ring.
	if got := popReadyTestStreams(t, ws, maxFrameSize); !reflect.DeepEqual(got, []uint32{3}) {
		t.Fatalf("popped streams %v, want [3]", got)
	}
	blocked.flow.add(maxFrameSize)
	if got := popReadyTestStreams(t, ws, maxFrameSize); !reflect.DeepEqual(got, []uint32{1}) {
		t.Fatalf("popped streams %v, want [1]", got)
	}
}
