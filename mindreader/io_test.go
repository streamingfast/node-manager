package mindreader

import (
	"context"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/merger/bundle"
)

type TestArchiverIO struct {
	MergeAndStoreFunc            func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error)
	FetchMergedOneBlockFilesFunc func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error)
	WalkOneBlockFilesFunc        func(ctx context.Context, callback func(*bundle.OneBlockFile) error) (err error)
	DownloadOneBlockFileFunc     func(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error)

	StoreOneBlockFileFunc            func(ctx context.Context, fileName string, block *bstream.Block) error
	StoreMergeableOneBlockFileFunc   func(ctx context.Context, fileName string, block *bstream.Block) error
	DeleteOneBlockFilesFunc          func(oneBlockFiles []*bundle.OneBlockFile)
	WalkMergeableOneBlockFilesFunc   func(ctx context.Context) ([]*bundle.OneBlockFile, error)
	SendMergeableAsOneBlockFilesFunc func(ctx context.Context) error
}

func (io *TestArchiverIO) SendMergeableAsOneBlockFiles(ctx context.Context) error {
	if io.SendMergeableAsOneBlockFilesFunc == nil {
		return nil
	}
	return io.SendMergeableAsOneBlockFilesFunc(ctx)
}

func (io *TestArchiverIO) MergeAndStore(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
	if io.MergeAndStoreFunc == nil {
		return nil
	}
	return io.MergeAndStoreFunc(inclusiveLowerBlock, oneBlockFiles)
}

func (io *TestArchiverIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
	if io.FetchMergedOneBlockFilesFunc == nil {
		return nil, nil
	}
	return io.FetchMergedOneBlockFilesFunc(lowBlockNum)
}

func (io *TestArchiverIO) WalkOneBlockFiles(ctx context.Context, callback func(*bundle.OneBlockFile) error) (err error) {
	if io.WalkOneBlockFilesFunc == nil {
		return nil
	}
	return io.WalkOneBlockFilesFunc(ctx, callback)
}

func (io *TestArchiverIO) DownloadOneBlockFile(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error) {
	if io.DownloadOneBlockFileFunc == nil {
		return nil, nil
	}
	return io.DownloadOneBlockFileFunc(ctx, oneBlockFile)
}

func (io *TestArchiverIO) StoreOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	if io.StoreOneBlockFileFunc == nil {
		return nil
	}
	return io.StoreOneBlockFileFunc(ctx, fileName, block)
}
func (io *TestArchiverIO) StoreMergeableOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	if io.StoreMergeableOneBlockFileFunc == nil {
		return nil
	}
	return io.StoreMergeableOneBlockFileFunc(ctx, fileName, block)
}

func (io *TestArchiverIO) Delete(oneBlockFiles []*bundle.OneBlockFile) {
	if io.DeleteOneBlockFilesFunc == nil {
		return
	}
	io.DeleteOneBlockFilesFunc(oneBlockFiles)
}

func (io *TestArchiverIO) WalkMergeableOneBlockFiles(ctx context.Context) ([]*bundle.OneBlockFile, error) {
	if io.WalkMergeableOneBlockFilesFunc == nil {
		return nil, nil
	}

	return io.WalkMergeableOneBlockFilesFunc(ctx)
}
