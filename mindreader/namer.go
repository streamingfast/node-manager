package mindreader

import (
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
)

func shortBlockID(in string) string {
	if len(in) > 8 {
		return in[len(in)-8:]
	}
	return in
}

func blockFileName(block *bstream.Block, suffix string) string {

	blockID := shortBlockID(block.ID())

	previousID := shortBlockID(block.PreviousID())

	if suffix == "" {
		suffix = "default"
	}

	return blockFileNameFromArgs(block.Num(), block.Time(), blockID, previousID, block.LibNum, suffix)

}

func blockFileNameFromArgs(num uint64, t time.Time, id string, prevID string, libNum uint64, suffix string) string {
	timeString := fmt.Sprintf("%s.%01d", t.Format("20060102T150405"), t.Nanosecond()/100000000)
	return fmt.Sprintf("%010d-%s-%s-%s-%d-%s", num, timeString, id, prevID, libNum, suffix)
}
