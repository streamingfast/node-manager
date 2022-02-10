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
	SaveOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error
	DeleteOneBlockFiles(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error
	WalkOneBlockFiles(ctx context.Context) []*bundle.OneBlockFile
}

type ArchiverDStoreIO struct {
	blockWriterFactory bstream.BlockWriterFactory
	blockReaderFactory bstream.BlockReaderFactory

	oneBlockStore     dstore.Store
	mergedBlocksStore dstore.Store

	oneBlockDeleter merger.OneBlockFilesDeleter
	mergerIO        merger.IOInterface
}

func NewArchiverDStoreIO(
	blockWriterFactory bstream.BlockWriterFactory,
	blockReaderFactory bstream.BlockReaderFactory,
	oneBlocksStore dstore.Store,
	mergedBlocksStore dstore.Store,
	maxOneBlockOperationsBatchSize int,
	retryAttempts int,
	retryCooldown time.Duration,
) *ArchiverDStoreIO {
	return &ArchiverDStoreIO{
		blockWriterFactory: blockWriterFactory,
		blockReaderFactory: blockReaderFactory,
		oneBlockStore:      oneBlocksStore,
		mergedBlocksStore:  mergedBlocksStore,
		oneBlockDeleter:    merger.NewOneBlockFilesDeleter(oneBlocksStore),
		mergerIO:           merger.NewDStoreIO(oneBlocksStore, mergedBlocksStore, maxOneBlockOperationsBatchSize, retryAttempts, retryCooldown),
	}
}

func (m *ArchiverDStoreIO) MergeAndSave(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
	return m.mergerIO.MergeAndSave(inclusiveLowerBlock, oneBlockFiles)
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

func (m *ArchiverDStoreIO) SaveOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	buffer := bytes.NewBuffer(nil)
	blockWriter, err := m.blockWriterFactory.New(buffer)
	if err != nil {
		return fmt.Errorf("write block factory: %w", err)
	}

	if err := blockWriter.Write(block); err != nil {
		return fmt.Errorf("write block: %w", err)
	}

	return m.oneBlockStore.WriteObject(ctx, fileName, buffer)
}

func (m *ArchiverDStoreIO) DeleteOneBlockFiles(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error {
	m.oneBlockDeleter.Delete(oneBlockFiles)
	return nil
}

func (m *ArchiverDStoreIO) WalkOneBlockFiles(ctx context.Context) []*bundle.OneBlockFile {
	var files []*bundle.OneBlockFile
	m.oneBlockStore.Walk(ctx, "", "", func(filename string) (err error) {
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
		files = append(files, obf)

		return nil
	})

	return files
}
