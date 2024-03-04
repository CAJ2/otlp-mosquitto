package main

import (
	"context"
	"fmt"
	"log"
	"syscall"
	"time"
	"unsafe"

	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/metric"
)

var msgQueueCounter metric.Int64UpDownCounter
var msgQueueNum int64
var msgQueueTicker *time.Ticker

func initMsgQueue() error {
	var err error
	useMsgQueue := viper.GetBool("msgqueue")
	if useMsgQueue {
		msgQueueCounter, err = meter.Int64UpDownCounter("ipcs/msg/qnum")
		if err != nil {
			return fmt.Errorf("failed to create msg queue counter: %w", err)
		}
		msgQueueTicker = time.NewTicker(1 * time.Minute)
		go msgQueue()
	}
	return nil
}

const MSG_STAT = 11

func msgQueue() {
	var errno syscall.Errno
	for {
		select {
		case <-msgQueueTicker.C:
			convertedBuffer := MsqidDS{}
			var realBuffer msqidds
			_, _, errno = syscall.Syscall(
				syscall.SYS_MSGCTL,
				uintptr(0),
				uintptr(MSG_STAT),
				uintptr(unsafe.Pointer(&realBuffer)))
			if errno != 0 {
				log.Printf("msg queue error: %s", errno)
			}
			realBuffer.convertBuffer(&convertedBuffer)

			msgQueueCounter.Add(context.Background(), int64(convertedBuffer.MsgQnum)-msgQueueNum)
			msgQueueNum = int64(convertedBuffer.MsgQnum)
		}
	}
}

// Permission define the Linux permission for a specific queue,
// private data is required to read/write buffer of constant length
type Permission struct {
	Key     uint32
	Uid     uint32
	Gid     uint32
	Cuid    uint32
	Cgid    uint32
	Mode    uint16
	pad1    uint16
	Seq     uint16
	pad2    uint16
	unused1 uint64
	unused2 uint64
}

// MsqidDS struct type define one of the buffer for MsgctlExtend
type MsqidDS struct {
	MsgPerm   Permission
	MsgStime  time.Time
	MsgRtime  time.Time
	MsgCtime  time.Time
	MsgCbytes uint64
	MsgQnum   uint64
	MsgQbytes uint64
	MsglSpid  uint32
	MsglRpid  uint32
}

// Msginfo struct type define one of the buffer for MsgctlExtend
type Msginfo struct {
	Msgpool int32
	Msgmap  int32
	Msgmax  int32
	Msgmnb  int32
	Msgmni  int32
	Msgssz  int32
	Msgtql  int32
	Msgseg  uint16
}

type ipctime uint64

// msqidds internal data for buffer conversion
type msqidds struct {
	msgPerm   Permission
	msgStime  ipctime
	msgRtime  ipctime
	msgCtime  ipctime
	msgCbytes uint64
	msgQnum   uint64
	msgQbytes uint64
	msglSpid  uint32
	msglRpid  uint32
	unused    [8]uint8
}

// convertBuffer from private to public data conversion
func (m *msqidds) convertBuffer(r *MsqidDS) {
	r.MsgPerm = m.msgPerm
	r.MsgStime = time.Unix(int64(0), int64(m.msgStime))
	r.MsgRtime = time.Unix(int64(0), int64(m.msgRtime))
	r.MsgCtime = time.Unix(int64(0), int64(m.msgCtime))
	r.MsgCbytes = m.msgCbytes
	r.MsgQnum = m.msgQnum
	r.MsgQbytes = m.msgQbytes
	r.MsglSpid = m.msglSpid
	r.MsglRpid = m.msglRpid
}

// convertBuffer from public to private data conversion
func (m *MsqidDS) convertBuffer(r *msqidds) {
	r.msgPerm = m.MsgPerm
	r.msgStime = ipctime(m.MsgStime.Nanosecond())
	r.msgRtime = ipctime(m.MsgRtime.Nanosecond())
	r.msgCtime = ipctime(m.MsgCtime.Nanosecond())
	r.msgCbytes = m.MsgCbytes
	r.msgQnum = m.MsgQnum
	r.msgQbytes = m.MsgQbytes
	r.msglSpid = m.MsglSpid
	r.msglRpid = m.MsglRpid
}
