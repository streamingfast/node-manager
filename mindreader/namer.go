package mindreader

import (
	"fmt"

	"github.com/dfuse-io/bstream"
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

	suffixString := ""
	if suffix != "" {
		suffixString = fmt.Sprintf("-%s", suffix)
	}
	return fmt.Sprintf("%010d-%s-%s-%s%s", block.Num(), blockTimeString, blockID, previousID, suffixString)
}
