package mindreader

import (
	"context"
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileUploader(t *testing.T) {
	localStore := dstore.NewMockStore(nil)
	localStore.SetFile("test1", nil)
	localStore.SetFile("test2", nil)
	localStore.SetFile("test3", nil)

	destinationStore := dstore.NewMockStore(nil)

	var destinationTestFiles []string
	done := make(chan interface{})
	destinationStore.PushLocalFileFunc = func(ctx context.Context, localFile, toBaseName string) (err error) {
		destinationTestFiles = append(destinationTestFiles, localFile)
		if len(destinationTestFiles) == 3 {
			close(done)
		}

		return nil
	}

	uploader := NewFileUploader(localStore, destinationStore, testLogger)
	err := uploader.uploadFiles(context.Background())
	require.NoError(t, err)

	select {
	case <-done:
		assert.Equal(t, 3, len(destinationTestFiles))
	case <-time.After(5 * time.Second):
		t.Error("took took long")
	}
}
