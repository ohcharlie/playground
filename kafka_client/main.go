package main

import (
	"bytes"
	"hash/crc32"
	"kafka_client/kafka"
	"log/slog"
	"net"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "192.168.1.206:9092")
	if err != nil {
		slog.Error("failed to connect kafka leader", "error", err)
		return
	}

	clientID := "my_kafka_client"
	firstMsgTimestamp := time.Now().UnixMilli()
	lastMsgTimestamp := firstMsgTimestamp

	firstRecord := &kafka.Record{
		Length:         kafka.New_VARINT(0), // 后面被填充计算
		Attributes:     kafka.INT8(0),
		TimestampDelta: kafka.New_VARLONG(int64(0)),
		OffsetDelta:    kafka.New_VARINT(int32(0)),
		KeyLength:      kafka.New_VARINT(int32(len([]byte("charlie666")))),
		Key:            kafka.New_RecordBYTES([]byte("charlie666")),
		ValueLength:    kafka.New_VARINT(int32(len([]byte("hello from charlie666, good night aha")))),
		Value:          kafka.New_RecordBYTES([]byte("hello from charlie666, good night aha")),
		HeadersCount:   kafka.New_VARINT(0),
		Headers: kafka.New_RecordARRAY(
			[]*kafka.RecordHeader{},
		),
	}
	firstRecord.Length = kafka.New_VARINT(firstRecord.Size())

	firstRecordBatch := &kafka.RecordBatch{
		Length:               kafka.INT32(0), // 后面被填充
		BaseOffset:           kafka.INT64(0),
		BatchLength:          0, // 后面被填充
		PartitionLeaderEpoch: kafka.INT32(-1),
		Magic:                kafka.INT8(2),
		Crc:                  kafka.UINT32(0), // 后面被填充
		Attributes:           kafka.INT16(0),
		// 当前record batch最后一条record的offset
		// 一个record batch中第一条record, OffsetDelta为0， 第二条为1, ...
		// 当前record batch最后一条offsetdelta为 len(currentRecordBatch records) - 1
		LastOffsetDelta: kafka.INT32(0),
		BaseTimestamp:   kafka.INT64(firstMsgTimestamp),
		MaxTimestamp:    kafka.INT64(lastMsgTimestamp),
		// 事务相关
		ProducerId:    kafka.INT64(-1),
		ProducerEpoch: kafka.INT16(-1),
		BaseSequence:  kafka.INT32(-1),

		RecordsCount: kafka.INT32(1),
		Records: kafka.New_RecordARRAY(
			[]*kafka.Record{
				firstRecord,
			},
		),
	}

	firstRecordBatch.Length = kafka.INT32(firstRecordBatch.Size() + firstRecord.Length.Size())
	firstRecordBatch.BatchLength = kafka.INT32(firstRecordBatch.Size() + firstRecord.Length.Size() -
		int32(firstRecordBatch.BaseOffset.Size()) - int32(firstRecordBatch.BatchLength.Size()))

	req := kafka.ProduceRequestV7{
		Length: 0, // To be calculated later
		RequestHeader: kafka.ProduceRequestV7_RequestHeader{
			Request_api_key:     kafka.INT16(int16(0)),
			Request_api_version: kafka.INT16(int16(7)),
			Correlation_id:      kafka.INT32(1),
			Client_id:           kafka.New_NULLABLE_STRING(&clientID),
		},
		RequestBody: kafka.ProduceRequestV7_RequestBody{
			Transactional_id: kafka.New_NULLABLE_STRING(nil),
			Acks:             kafka.INT16(-1),
			Timeout_ms:       kafka.INT32(900000),
			Topic_data_array: kafka.New_ARRAY(
				[]*kafka.ProduceRequestV7_TopicData{
					{
						Name: kafka.New_STRING("auth.user.created"),
						Partition_data_array: kafka.New_ARRAY(
							[]*kafka.ProduceRequestV7_PartitionData{
								{
									Index:       kafka.INT32(0),
									RecordBatch: firstRecordBatch,
								},
							},
						),
					},
				},
			),
		},
	}
	req.Length = kafka.INT32(req.Size() + firstRecord.Length.Size() + 4)

	bufWriter := new(bytes.Buffer)
	firstRecordBatch.Attributes.Encode(bufWriter)
	firstRecordBatch.LastOffsetDelta.Encode(bufWriter)
	firstRecordBatch.BaseTimestamp.Encode(bufWriter)
	firstRecordBatch.MaxTimestamp.Encode(bufWriter)
	firstRecordBatch.ProducerId.Encode(bufWriter)
	firstRecordBatch.ProducerEpoch.Encode(bufWriter)
	firstRecordBatch.BaseSequence.Encode(bufWriter)
	firstRecordBatch.RecordsCount.Encode(bufWriter)
	firstRecordBatch.Records.Encode(bufWriter)

	data := bufWriter.Bytes()
	crc32Tab := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(data, crc32Tab)
	firstRecordBatch.Crc = kafka.UINT32(checksum)

	req.Encode(conn)
}

/*
	Request => MessageSize(int32) RequestMessage
		RequestMessage => RequestHeader Body
			RequestHeader => request_api_key(INT16) request_api_version(INT16) correlation_id(INT32) client_id(NULLABLE_STRING)
			Body => transactional_id(NULLABLE_STRING) acks(INT16) timeout_ms(INT32) [topic_data]
				topic_data => name(STRING) [partition_data]
					partition_data => index(INT32) records

	records format:

	RecordBatch =>

	baseOffset: int64
	batchLength: int32
	partitionLeaderEpoch: int32
	magic: int8 (current magic value is 2)
	crc: uint32
	attributes: int16
		bit 0~2:
			0: no compression
			1: gzip
			2: snappy
			3: lz4
			4: zstd
		bit 3: timestampType
		bit 4: isTransactional (0 means not transactional)
		bit 5: isControlBatch (0 means not a control batch)
		bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
		bit 7~15: unused
		lastOffsetDelta: int32
		baseTimestamp: int64
		maxTimestamp: int64
		producerId: int64
		producerEpoch: int16
		baseSequence: int32
		recordsCount: int32
		records: [Record]

	Record =>

	length: varint
	attributes: int8
		bit 0~7: unused
	timestampDelta: varlong
	offsetDelta: varint
	keyLength: varint
	key: byte[]
	valueLength: varint
	value: byte[]
	headersCount: varint
	Headers => [Header]

	RecordHeader =>

	headerKeyLength: varint
	headerKey: String
	headerValueLength: varint
	Value: byte[]
*/
