package mindreader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFileUploader(t *testing.T) {
	localStore := &TestStore{}
	localTestFiles := []string{"test1", "test2", "test3"}
	localStore.WalkFunc = func(ctx context.Context, prefix, ignoreSuffix string, f func(filename string) (err error)) error {
		for _, testFile := range localTestFiles {
			f(testFile)
		}
		return nil
	}
	localStore.ObjectPathFunc = func(s string) string {
		return s
	}

	destinationStore := &TestStore{}
	var destinationTestFiles []string
	done := make(chan interface{})
	destinationStore.PushLocalFileFunc = func(ctx context.Context, localFile, toBaseName string) (err error) {
		destinationTestFiles = append(destinationTestFiles, localFile)
		if len(destinationTestFiles) == 3 {
			close(done)
		}
		return nil
	}
	destinationStore.ObjectPathFunc = func(s string) string {
		return s
	}

	uploader := NewFileUploader(localStore, destinationStore, zap.NewNop())
	err := uploader.uploadFiles(context.Background())
	require.NoError(t, err)

	select {
	case <-done:
		assert.Equal(t, 3, len(destinationTestFiles))
	case <-time.After(5 * time.Second):
		t.Error("took took long")
	}
}
