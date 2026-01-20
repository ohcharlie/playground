package kafka

import (
	"encoding/binary"
	"io"
)

// Ref: https://kafka.apache.org/41/design/protocol/#protocol-primitive-types

type Type interface {
	Size() int32
	Encode(w io.Writer)
}

type INT8 int8

type INT16 int32

type INT32 int32
type INT64 int64

type UINT32 uint32
type ARRAY[T Type] struct {
	arr []T
}

type RecordARRAY[T Type] struct {
	arr []T
}

/*
Represents an integer between -2^31 and 2^31-1 inclusive. Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
*/
type VARINT struct {
	v int32
}

func New_VARINT(v int32) VARINT {
	return VARINT{v: v}
}

/*
Represents an integer between -2^63 and 2^63-1 inclusive. Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
*/
type VARLONG struct {
	v int64
}

func New_VARLONG(v int64) VARLONG {
	return VARLONG{v: v}
}

type STRING struct {
	s string
}

func New_STRING(s string) STRING {
	return STRING{s: s}
}

type NULLABLE_STRING struct {
	s *STRING
}

func New_NULLABLE_STRING(s *string) NULLABLE_STRING {
	if s == nil {
		return NULLABLE_STRING{s: nil}
	}
	p := New_STRING(*s)
	return NULLABLE_STRING{s: &p}
}

type BYTES struct {
	bytes []byte
}

type RecordBYTES struct {
	bytes []byte
}

func New_BYTES(bytes []byte) BYTES {
	return BYTES{bytes: bytes}
}
func New_RecordBYTES(bytes []byte) RecordBYTES {
	return RecordBYTES{bytes: bytes}
}

func (x INT8) Size() int32 {
	return 1
}
func (x INT16) Size() int32 {
	return 2
}
func (x INT32) Size() int32 {
	return 4
}
func (x INT64) Size() int32 {
	return 8
}

func (x UINT32) Size() int32 {
	return 4
}

func (x ARRAY[T]) Size() int32 {
	if len(x.arr) == 0 {
		return 4
	}
	var ret int32 = 4
	for _, e := range x.arr {
		ret += e.Size()
	}
	return ret
}

func (x RecordARRAY[T]) Size() int32 {
	var ret int32 = 0
	for _, e := range x.arr {
		ret += e.Size()
		// TODO
	}
	return ret
}

func (x VARINT) Size() int32 {
	return varintLen(int64(x.v))
}
func (x VARLONG) Size() int32 {
	return varintLen(x.v)
}

func (x STRING) Size() int32 {
	return 2 + int32(len(x.s))
}
func (x NULLABLE_STRING) Size() int32 {
	if x.s == nil {
		return 2
	}
	return x.s.Size()
}
func (x BYTES) Size() int32 {
	return 4 + int32(len(x.bytes))
}
func (x RecordBYTES) Size() int32 {
	return int32(len(x.bytes))
}

func (x INT8) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, int8(x))
}
func (x INT16) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, int16(x))
}
func (x INT32) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, int32(x))
}
func (x INT64) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, int64(x))
}

func (x UINT32) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, uint32(x))
}

func (x VARINT) Encode(w io.Writer) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(x.v))
	binary.Write(w, binary.BigEndian, buf[:n])
}
func (x VARLONG) Encode(w io.Writer) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, x.v)
	binary.Write(w, binary.BigEndian, buf[:n])
}

func (x STRING) Encode(w io.Writer) {
	b := []byte(x.s)
	binary.Write(w, binary.BigEndian, int16(len(b)))
	w.Write(b)
}
func (x NULLABLE_STRING) Encode(w io.Writer) {
	if x.s == nil {
		binary.Write(w, binary.BigEndian, int16(-1))
	} else {
		x.s.Encode(w)
	}
}
func (x BYTES) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, int32(len(x.bytes)))
	w.Write(x.bytes)
}
func (x RecordBYTES) Encode(w io.Writer) {
	w.Write(x.bytes)
}
func (x ARRAY[T]) Encode(w io.Writer) {
	if x.arr == nil {
		binary.Write(w, binary.BigEndian, int32(-1))
		return
	}
	binary.Write(w, binary.BigEndian, int32(len(x.arr)))
	for _, e := range x.arr {
		e.Encode(w)
	}
}
func (x RecordARRAY[T]) Encode(w io.Writer) {
	for _, e := range x.arr {
		e.Encode(w)
	}
}

// helper
func uVarintLen(x uint64) int32 {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return int32(i + 1)
}
func varintLen(x int64) int32 {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return uVarintLen(ux)
}
