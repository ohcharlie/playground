package kafka

import (
	"io"
)

type ProduceRequestV7 struct {
	Length        INT32
	RequestHeader ProduceRequestV7_RequestHeader
	RequestBody   ProduceRequestV7_RequestBody
}

func (t *ProduceRequestV7) Size() int32 {
	// 不包含Length字段的size
	return t.RequestHeader.Size() + t.RequestBody.Size()
}

func (t *ProduceRequestV7) Encode(w io.Writer) {
	t.Length.Encode(w)
	t.RequestHeader.Encode(w)
	t.RequestBody.Encode(w)
}

type ProduceRequestV7_RequestHeader struct {
	Request_api_key     INT16
	Request_api_version INT16
	Correlation_id      INT32
	Client_id           NULLABLE_STRING
}

func (h *ProduceRequestV7_RequestHeader) Size() int32 {
	return h.Request_api_key.Size() +
		h.Request_api_version.Size() +
		h.Correlation_id.Size() +
		h.Client_id.Size()
}

func (h *ProduceRequestV7_RequestHeader) Encode(w io.Writer) {
	h.Request_api_key.Encode(w)
	h.Request_api_version.Encode(w)
	h.Correlation_id.Encode(w)
	h.Client_id.Encode(w)
}

type ProduceRequestV7_RequestBody struct {
	Transactional_id NULLABLE_STRING
	Acks             INT16
	Timeout_ms       INT32
	Topic_data_array ARRAY[*ProduceRequestV7_TopicData]
}

func New_ARRAY[T Type](array []T) ARRAY[T] {
	return ARRAY[T]{
		arr: array,
	}
}

func New_RecordARRAY[T Type](array []T) RecordARRAY[T] {
	return RecordARRAY[T]{
		arr: array,
	}
}

func (t *ProduceRequestV7_RequestBody) Size() int32 {
	return t.Transactional_id.Size() +
		t.Acks.Size() +
		t.Timeout_ms.Size() +
		t.Topic_data_array.Size()
}

func (t *ProduceRequestV7_RequestBody) Encode(w io.Writer) {
	t.Transactional_id.Encode(w)
	t.Acks.Encode(w)
	t.Timeout_ms.Encode(w)
	t.Topic_data_array.Encode(w)
}

type ProduceRequestV7_TopicData struct {
	Name                 STRING
	Partition_data_array ARRAY[*ProduceRequestV7_PartitionData]
}

func (t *ProduceRequestV7_TopicData) Size() int32 {
	return t.Name.Size() + t.Partition_data_array.Size()
}

func (t *ProduceRequestV7_TopicData) Encode(w io.Writer) {
	t.Name.Encode(w)
	t.Partition_data_array.Encode(w)
}

type ProduceRequestV7_PartitionData struct {
	Index       INT32
	RecordBatch *RecordBatch
}

func (t *ProduceRequestV7_PartitionData) Size() int32 {
	return t.Index.Size() + t.RecordBatch.Size()
}

func (t *ProduceRequestV7_PartitionData) Encode(w io.Writer) {
	t.Index.Encode(w)
	t.RecordBatch.Encode(w)
}

// Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see Message Sets.
type RecordBatch struct {
	Length INT32 // property of NULLABLE_BYTES

	BaseOffset           INT64
	BatchLength          INT32
	PartitionLeaderEpoch INT32
	Magic                INT8
	Crc                  UINT32
	Attributes           INT16
	LastOffsetDelta      INT32
	BaseTimestamp        INT64
	MaxTimestamp         INT64
	ProducerId           INT64
	ProducerEpoch        INT16
	BaseSequence         INT32
	RecordsCount         INT32
	// Records              ARRAY[Record]
	Records RecordARRAY[*Record]
}

func (r *RecordBatch) Size() int32 {
	return r.BaseOffset.Size() +
		r.BatchLength.Size() +
		r.PartitionLeaderEpoch.Size() +
		r.Magic.Size() +
		r.Crc.Size() +
		r.Attributes.Size() +
		r.LastOffsetDelta.Size() +
		r.BaseTimestamp.Size() +
		r.MaxTimestamp.Size() +
		r.ProducerId.Size() +
		r.ProducerEpoch.Size() +
		r.BaseSequence.Size() +
		r.RecordsCount.Size() +
		r.Records.Size()
}

func (r *RecordBatch) Encode(w io.Writer) {
	r.Length.Encode(w)
	r.BaseOffset.Encode(w)
	r.BatchLength.Encode(w)
	r.PartitionLeaderEpoch.Encode(w)
	r.Magic.Encode(w)
	r.Crc.Encode(w)
	r.Attributes.Encode(w)
	r.LastOffsetDelta.Encode(w)
	r.BaseTimestamp.Encode(w)
	r.MaxTimestamp.Encode(w)
	r.ProducerId.Encode(w)
	r.ProducerEpoch.Encode(w)
	r.BaseSequence.Encode(w)
	r.RecordsCount.Encode(w)
	r.Records.Encode(w)
}

type Record struct {
	Length VARINT

	Attributes     INT8
	TimestampDelta VARLONG
	OffsetDelta    VARINT
	KeyLength      VARINT
	Key            RecordBYTES
	ValueLength    VARINT
	Value          RecordBYTES
	HeadersCount   VARINT
	Headers        RecordARRAY[*RecordHeader]
}

func (r *Record) Size() int32 {
	return 0 +
		r.Attributes.Size() +
		r.TimestampDelta.Size() +
		r.OffsetDelta.Size() +
		r.KeyLength.Size() +
		r.Key.Size() +
		r.ValueLength.Size() +
		r.Value.Size() +
		r.HeadersCount.Size() +
		r.Headers.Size()
}

func (r *Record) Encode(w io.Writer) {
	r.Length.Encode(w)
	r.Attributes.Encode(w)
	r.TimestampDelta.Encode(w)
	r.OffsetDelta.Encode(w)
	r.KeyLength.Encode(w)
	r.Key.Encode(w)
	r.ValueLength.Encode(w)
	r.Value.Encode(w)
	r.HeadersCount.Encode(w)
	r.Headers.Encode(w)
}

type RecordHeader struct {
	HeaderKeyLength   VARINT
	HeaderKey         STRING
	HeaderValueLength VARINT
	Value             BYTES
}

func (r *RecordHeader) Size() int32 {
	// TODO
	return 0
}

func (r *RecordHeader) Encode(w io.Writer) {
	// TODO
}
