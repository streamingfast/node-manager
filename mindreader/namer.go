package mindreader

import (
	"fmt"

	"github.com/streamingfast/bstream"
)

func blockFileName(block *bstream.Block, suffix string) string {
	blockTime := block.Time()
	blockTimeString := fmt.Sprintf("%s.%01d", blockTime.Format("20060102T150405"), blockTime.Nanosecond()/100000000)

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

	return fmt.Sprintf("%010d-%s-%s-%s-%d-%s", block.Num(), blockTimeString, blockID, previousID, block.LibNum, suffix)
}
