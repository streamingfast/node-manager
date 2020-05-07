package manageos

import "time"

type HeadBlockUpdater func(*HeadBlock)

type HeadBlock struct {
	ID   string
	Num  uint64
	Time time.Time
}

type Monitor interface {
	IsReady() bool
}
