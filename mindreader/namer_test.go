package mindreader

import (
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/require"
)

func TestNamer_blockFileName(t *testing.T) {

	blockTime, err := time.Parse("20060102T150405", "20211231T123055")
	require.NoError(t, err)

	cases := []struct {
		name             string
		expectedFileName string
		block            *bstream.Block
		suffix           string
	}{
		{
			name: "No suffix",
			block: &bstream.Block{
				Id:             "00000101a",
				Number:         1,
				PreviousId:     "00000100a",
				Timestamp:      blockTime,
				LibNum:         99,
				PayloadKind:    0,
				PayloadVersion: 0,
				PayloadBuffer:  nil,
			},
			suffix:           "",
			expectedFileName: "0000000001-20211231T123055.0-0000101a-0000100a-99",
		},
		{
			name: "With suffix",
			block: &bstream.Block{
				Id:             "00000101a",
				Number:         1,
				PreviousId:     "00000100a",
				Timestamp:      blockTime,
				LibNum:         99,
				PayloadKind:    0,
				PayloadVersion: 0,
				PayloadBuffer:  nil,
			},
			suffix:           "suffix",
			expectedFileName: "0000000001-20211231T123055.0-0000101a-0000100a-99-suffix",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fileName := blockFileName(c.block, c.suffix)
			require.Equal(t, c.expectedFileName, fileName)
		})
	}
}
