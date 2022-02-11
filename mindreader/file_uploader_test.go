package mindreader

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
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
	destinationStore := &TestStore{}
	destinationTestFiles := []string{}
	destinationStore.PushLocalFileFunc = func(ctx context.Context, localFile, toBaseName string) (err error) {
		destinationTestFiles = append(destinationTestFiles, localFile)
		return nil
	}

	uploader := NewFileUploader(localStore, destinationStore, zap.NewNop())
	uploader.uploadFiles(context.Background())

	assert.Equal(t, 3, len(destinationTestFiles))
}
