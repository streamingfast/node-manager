package node_manager

import (
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmetrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Readiness interface {
	IsReady() bool
}

type MetricsAndReadinessManager struct {
	headBlockChan      chan *bstream.Block
	headBlockTimeDrift *dmetrics.HeadTimeDrift
	headBlockNumber    *dmetrics.HeadBlockNum
	readinessProbe     *atomic.Bool

	// ReadinessMaxLatency is the max delta between head block time and
	// now before /healthz starts returning success
	readinessMaxLatency time.Duration

	logger *zap.Logger
}

func NewMetricsAndReadinessManager(headBlockTimeDrift *dmetrics.HeadTimeDrift, headBlockNumber *dmetrics.HeadBlockNum, readinessMaxLatency time.Duration) *MetricsAndReadinessManager {
	return &MetricsAndReadinessManager{
		headBlockChan:       make(chan *bstream.Block, 1), // just for non-blocking, saving a few nanoseconds here
		readinessProbe:      atomic.NewBool(false),
		headBlockTimeDrift:  headBlockTimeDrift,
		headBlockNumber:     headBlockNumber,
		readinessMaxLatency: readinessMaxLatency,
	}
}

func (m *MetricsAndReadinessManager) setReadinessProbeOn() {
	if m.readinessProbe.CAS(false, true) {
		if m.logger != nil {
			m.logger.Info("nodeos superviser is now assumed to be ready")
		}
	}
}

func (m *MetricsAndReadinessManager) setReadinessProbeOff() {
	if m.readinessProbe.CAS(true, false) {
		if m.logger != nil {
			m.logger.Info("nodeos superviser is now assumed to be unavailable (not ready)")
		}
	}
}

func (m *MetricsAndReadinessManager) IsReady() bool {
	return m.readinessProbe.Load()
}

func (m *MetricsAndReadinessManager) Launch() {
	for {
		var lastSeenBlock *bstream.Block
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
			m.headBlockNumber.SetUint64(lastSeenBlock.Num())
		}

		if lastSeenBlock.Time().IsZero() { // never act upon zero timestamps
			continue
		}
		if m.headBlockTimeDrift != nil {
			m.headBlockTimeDrift.SetBlockTime(lastSeenBlock.Time())
		}

		// readiness
		if m.readinessMaxLatency == 0 || time.Since(lastSeenBlock.Time()) < m.readinessMaxLatency {
			m.setReadinessProbeOn()
		} else {
			m.setReadinessProbeOff()
		}
	}
}

func (m *MetricsAndReadinessManager) UpdateHeadBlock(block *bstream.Block) error {
	m.headBlockChan <- block
	return nil
}
