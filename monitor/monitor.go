package monitor

import (
	"time"

	"github.com/dfuse-io/dmetrics"
	"github.com/dfuse-io/manageos"
	"go.uber.org/atomic"
)

type MetricsAndReadinessManager struct {
	headBlockChan      chan *manageos.HeadBlock
	headBlockTimeDrift *dmetrics.HeadTimeDrift
	headBlockNumber    *dmetrics.HeadBlockNum
	readinessProbe     *atomic.Bool

	// ReadinessMaxLatency is the max delta between head block time and
	// now before /healthz starts returning success
	readinessMaxLatency time.Duration
}

func NewMetricsAndReadinessManager(headBlockTimeDrift *dmetrics.HeadTimeDrift, headBlockNumber *dmetrics.HeadBlockNum, readinessMaxLatency time.Duration) *MetricsAndReadinessManager {
	return &MetricsAndReadinessManager{
		headBlockChan:       make(chan *manageos.HeadBlock),
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
	for {
		var lastSeenBlock *manageos.HeadBlock
		select {
		case block := <-m.headBlockChan:
			lastSeenBlock = block
		case <-time.After(500 * time.Millisecond):
		}

		if lastSeenBlock == nil {
			continue
		}

		if m.headBlockNumber != nil {
			m.headBlockNumber.SetUint64(lastSeenBlock.Num)
		}
		if m.headBlockTimeDrift != nil {
			m.headBlockTimeDrift.SetBlockTime(lastSeenBlock.Time)
		}

		if m.readinessMaxLatency == 0 || time.Since(lastSeenBlock.Time) < m.readinessMaxLatency {
			m.setReadinessProbeOn()
		} else {
			m.setReadinessProbeOff()
		}

	}
}

func (m *MetricsAndReadinessManager) UpdateHeadBlock(headBlock *manageos.HeadBlock) {
	// TODO: increase channel size to make it none blocking?
	m.headBlockChan <- headBlock
}
