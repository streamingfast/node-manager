package mindreader

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/merger"
	"github.com/streamingfast/merger/bundle"
)

type ArchiverIO interface {
	merger.IOInterface
	StoreMergeableOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error
	StoreOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error
	DeleteOneBlockFiles(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error
	WalkMergeableOneBlockFiles(ctx context.Context) (out []*bundle.OneBlockFile, err error)
}

type ArchiverDStoreIO struct {
	blockWriterFactory bstream.BlockWriterFactory
	blockReaderFactory bstream.BlockReaderFactory

	oneBlockStore          dstore.Store
	mergeableOneBlockStore dstore.Store

	localMergedBlocksStore dstore.Store
	mergedBlocksStore      dstore.Store

	oneBlockDeleter             merger.OneBlockFilesDeleter
	mergerIO                    merger.IOInterface
	uploadableOneBlockStore     dstore.Store
	uploadableMergedBlocksStore dstore.Store
}

func NewArchiverDStoreIO(
	blockWriterFactory bstream.BlockWriterFactory,
	blockReaderFactory bstream.BlockReaderFactory,
	oneBlocksStore dstore.Store,
	uploadableOneBlockStored dstore.Store,
	mergeableOneBlockStore dstore.Store,
	uploadableMergedBlocksStore dstore.Store,
	mergedBlocksStore dstore.Store,
	maxOneBlockOperationsBatchSize int,
	retryAttempts int,
	retryCooldown time.Duration,
) *ArchiverDStoreIO {
	return &ArchiverDStoreIO{
		blockWriterFactory:          blockWriterFactory,
		blockReaderFactory:          blockReaderFactory,
		mergeableOneBlockStore:      mergeableOneBlockStore,
		uploadableOneBlockStore:     uploadableOneBlockStored,
		uploadableMergedBlocksStore: uploadableMergedBlocksStore,
		oneBlockStore:               oneBlocksStore,
		mergedBlocksStore:           mergedBlocksStore,
		oneBlockDeleter:             merger.NewOneBlockFilesDeleter(mergeableOneBlockStore),
		mergerIO:                    merger.NewDStoreIO(mergeableOneBlockStore, uploadableMergedBlocksStore, maxOneBlockOperationsBatchSize, retryAttempts, retryCooldown),
	}
}

func (m *ArchiverDStoreIO) MergeAndStore(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
	return m.mergerIO.MergeAndStore(inclusiveLowerBlock, oneBlockFiles)
}

func (m *ArchiverDStoreIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
	return m.mergerIO.FetchMergedOneBlockFiles(lowBlockNum)
}

func (m *ArchiverDStoreIO) FetchOneBlockFiles(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
	return m.mergerIO.FetchOneBlockFiles(ctx)
}

func (m *ArchiverDStoreIO) DownloadOneBlockFile(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error) {
	return m.mergerIO.DownloadOneBlockFile(ctx, oneBlockFile)
}

func (m *ArchiverDStoreIO) StoreOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	return m.storeOneBlockFile(ctx, fileName, block, m.uploadableOneBlockStore)
}

func (m *ArchiverDStoreIO) StoreMergeableOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	return m.storeOneBlockFile(ctx, fileName, block, m.mergeableOneBlockStore)
}

func (m *ArchiverDStoreIO) storeOneBlockFile(ctx context.Context, fileName string, block *bstream.Block, store dstore.Store) error {
	buffer := bytes.NewBuffer(nil)
	blockWriter, err := m.blockWriterFactory.New(buffer)
	if err != nil {
		return fmt.Errorf("write block factory: %w", err)
	}

	if err := blockWriter.Write(block); err != nil {
		return fmt.Errorf("write block: %w", err)
	}

	return store.WriteObject(ctx, fileName, buffer)
}

func (m *ArchiverDStoreIO) DeleteOneBlockFiles(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error {
	m.oneBlockDeleter.Delete(oneBlockFiles)
	return nil
}

func (m *ArchiverDStoreIO) WalkMergeableOneBlockFiles(ctx context.Context) (out []*bundle.OneBlockFile, err error) {
	err = m.mergeableOneBlockStore.Walk(ctx, "", "", func(filename string) (err error) {
		blockNum, _, blockId, prevId, _, canonicalName, err := bundle.ParseFilename(filename)
		if err != nil {
			return fmt.Errorf("could not parse filename %s: %w", filename, err)
		}

		obf := &bundle.OneBlockFile{
			CanonicalName: canonicalName,
			ID:            blockId,
			Num:           blockNum,
			PreviousID:    prevId,
		}
		out = append(out, obf)

		return nil
	})

	return
}
