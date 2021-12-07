package mindreader

import (
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
)

func blockFileName(block *bstream.Block, suffix string) string {

	blockID := block.ID()
	if len(blockID) > 8 {
		blockID = blockID[len(blockID)-8:]
	}

	previousID := block.PreviousID()
	if len(previousID) > 8 {
		previousID = previousID[len(previousID)-8:]
	}

	if suffix == "" {
		suffix = "default"
	}

	return blockFileNameFromArgs(block.Num(), block.Time(), blockID, previousID, block.LibNum, suffix)

}

func blockFileNameFromArgs(num uint64, t time.Time, id string, prevID string, libNum uint64, suffix string) string {
	timeString := fmt.Sprintf("%s.%01d", t.Format("20060102T150405"), t.Nanosecond()/100000000)
	return fmt.Sprintf("%010d-%s-%s-%s-%d-%s", num, timeString, id, prevID, libNum, suffix)
}
