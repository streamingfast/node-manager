package node_manager

import (
	"time"

	"github.com/streamingfast/dmetrics"
	"go.uber.org/atomic"
)

type Readiness interface {
	IsReady() bool
}

type MetricsAndReadinessManager struct {
	headBlockChan      chan *headBlock
	headBlockTimeDrift *dmetrics.HeadTimeDrift
	headBlockNumber    *dmetrics.HeadBlockNum
	readinessProbe     *atomic.Bool

	// ReadinessMaxLatency is the max delta between head block time and
	// now before /healthz starts returning success
	readinessMaxLatency time.Duration
}

func NewMetricsAndReadinessManager(headBlockTimeDrift *dmetrics.HeadTimeDrift, headBlockNumber *dmetrics.HeadBlockNum, readinessMaxLatency time.Duration) *MetricsAndReadinessManager {
	return &MetricsAndReadinessManager{
		headBlockChan:       make(chan *headBlock, 1), // just for non-blocking, saving a few nanoseconds here
		readinessProbe:      atomic.NewBool(false),
		headBlockTimeDrift:  headBlockTimeDrift,
		headBlockNumber:     headBlockNumber,
		readinessMaxLatency: readinessMaxLatency,
	}
}

func (m *MetricsAndReadinessManager) setReadinessProbeOn() {
	if m.readinessProbe.CAS(false, true) {
		//m.Logger.Info("nodeos superviser is now assumed to be ready")
	}
}

func (m *MetricsAndReadinessManager) setReadinessProbeOff() {
	if m.readinessProbe.CAS(true, false) {
		//m.Logger.Info("nodeos superviser is not ready anymore")
	}
}

func (m *MetricsAndReadinessManager) IsReady() bool {
	return m.readinessProbe.Load()
}

func (m *MetricsAndReadinessManager) Launch() {
	var lastSeenBlock *headBlock
	for {
		select {
		case block := <-m.headBlockChan:
			lastSeenBlock = block
		case <-time.After(time.Second):
		}

		if lastSeenBlock == nil {
			continue
		}

		// metrics
		if m.headBlockNumber != nil {
			m.headBlockNumber.SetUint64(lastSeenBlock.Num)
		}

		if lastSeenBlock.Time.IsZero() { // never act upon zero timestamps
			continue
		}
		if m.headBlockTimeDrift != nil {
			m.headBlockTimeDrift.SetBlockTime(lastSeenBlock.Time)
		}

		// readiness
		if m.readinessMaxLatency == 0 || time.Since(lastSeenBlock.Time) < m.readinessMaxLatency {
			m.setReadinessProbeOn()
		} else {
			m.setReadinessProbeOff()
		}
	}
}

func (m *MetricsAndReadinessManager) UpdateHeadBlock(num uint64, ID string, t time.Time) {
	m.headBlockChan <- &headBlock{
		ID:   ID,
		Num:  num,
		Time: t,
	}
}

type headBlock struct {
	ID   string
	Num  uint64
	Time time.Time
}

type HeadBlockUpdater func(uint64, string, time.Time)
