package mindreader

import (
	"context"

	"github.com/streamingfast/bstream"
)

type TestArchiverIO struct {
	MergeAndStoreFunc            func(ctx context.Context, inclusiveLowerBlock uint64, oneBlockFiles []*bstream.OneBlockFile) (err error)
	FetchMergedOneBlockFilesFunc func(lowBlockNum uint64) ([]*bstream.OneBlockFile, error)
	WalkOneBlockFilesFunc        func(ctx context.Context, incusiveLowBlockNum uint64, callback func(*bstream.OneBlockFile) error) (err error)
	DownloadOneBlockFileFunc     func(ctx context.Context, oneBlockFile *bstream.OneBlockFile) (data []byte, err error)

	StoreOneBlockFileFunc            func(ctx context.Context, fileName string, block *bstream.Block) error
	StoreMergeableOneBlockFileFunc   func(ctx context.Context, fileName string, block *bstream.Block) error
	DeleteOneBlockFilesFunc          func(oneBlockFiles []*bstream.OneBlockFile)
	WalkMergeableOneBlockFilesFunc   func(ctx context.Context) ([]*bstream.OneBlockFile, error)
	SendMergeableAsOneBlockFilesFunc func(ctx context.Context) error
}

func (io *TestArchiverIO) SendMergeableAsOneBlockFiles(ctx context.Context) error {
	if io.SendMergeableAsOneBlockFilesFunc == nil {
		return nil
	}
	return io.SendMergeableAsOneBlockFilesFunc(ctx)
}

func (io *TestArchiverIO) NextBundle(_ context.Context, _ uint64) (uint64, bstream.BlockRef, error) {
	return 0, nil, nil
}

func (io *TestArchiverIO) MergeAndStore(ctx context.Context, inclusiveLowerBlock uint64, oneBlockFiles []*bstream.OneBlockFile) (err error) {
	if io.MergeAndStoreFunc == nil {
		return nil
	}
	return io.MergeAndStoreFunc(ctx, inclusiveLowerBlock, oneBlockFiles)
}

func (io *TestArchiverIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*bstream.OneBlockFile, error) {
	if io.FetchMergedOneBlockFilesFunc == nil {
		return nil, nil
	}
	return io.FetchMergedOneBlockFilesFunc(lowBlockNum)
}

func (io *TestArchiverIO) WalkOneBlockFiles(ctx context.Context, inclusiveLowBlockNum uint64, callback func(*bstream.OneBlockFile) error) (err error) {
	if io.WalkOneBlockFilesFunc == nil {
		return nil
	}
	return io.WalkOneBlockFilesFunc(ctx, inclusiveLowBlockNum, callback)
}

func (io *TestArchiverIO) DownloadOneBlockFile(ctx context.Context, oneBlockFile *bstream.OneBlockFile) (data []byte, err error) {
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

func (io *TestArchiverIO) DeleteAsync(oneBlockFiles []*bstream.OneBlockFile) {
	if io.DeleteOneBlockFilesFunc == nil {
		return
	}
	io.DeleteOneBlockFilesFunc(oneBlockFiles)
}

func (io *TestArchiverIO) WalkMergeableOneBlockFiles(ctx context.Context) ([]*bstream.OneBlockFile, error) {
	if io.WalkMergeableOneBlockFilesFunc == nil {
		return nil, nil
	}

	return io.WalkMergeableOneBlockFilesFunc(ctx)
}
