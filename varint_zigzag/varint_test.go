package main

import (
	"encoding/binary"
	"testing"
)

// Ref: https://protobuf.dev/programming-guides/encoding/

func TestVarintAndZigzag(t *testing.T) {
	tests := []struct {
		x         int64
		zigzagX   uint64
		varIntLen int
	}{
		{0, 0, 1},
		{-1, 1, 1},
		{1, 2, 1},
		{2, 4, 1},
		{-128, 255, 2},
		{128, 256, 2},
	}
	buf := make([]byte, binary.MaxVarintLen64)
	for _, tt := range tests {
		n := binary.PutVarint(buf, tt.x)
		gotX, l := binary.Varint(buf[:n])
		gotZigzagX, ul := binary.Uvarint(buf[:n])
		if l != tt.varIntLen || ul != tt.varIntLen || gotX != tt.x || gotZigzagX != tt.zigzagX {
			t.Errorf("got: (%d, %d), want: (%d, %d) ", gotX, gotZigzagX, tt.x, tt.zigzagX)
		}
	}
}
