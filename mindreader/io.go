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

var _ ArchiverIO = (*ArchiverDStoreIO)(nil) //compile-time check

type ArchiverIO interface {
	merger.IOInterface
	merger.OneBlockFilesDeleter
	StoreMergeableOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error
	StoreOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error
	WalkMergeableOneBlockFiles(ctx context.Context) (out []*bundle.OneBlockFile, err error)
}

type ArchiverDStoreIO struct {
	*merger.DStoreIO
	merger.OneBlockFilesDeleter

	blockWriterFactory bstream.BlockWriterFactory
	blockReaderFactory bstream.BlockReaderFactory

	oneBlockStore          dstore.Store
	mergeableOneBlockStore dstore.Store

	localMergedBlocksStore dstore.Store
	mergedBlocksStore      dstore.Store

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
		OneBlockFilesDeleter:        merger.NewOneBlockFilesDeleter(mergeableOneBlockStore),
		DStoreIO:                    merger.NewDStoreIO(mergeableOneBlockStore, uploadableMergedBlocksStore, maxOneBlockOperationsBatchSize, retryAttempts, retryCooldown),
	}
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

//func (m *ArchiverDStoreIO) DeleteOneBlockFiles(oneBlockFiles []*bundle.OneBlockFile) {
//	m.Delete(oneBlockFiles)
//}

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

		if obf.InnerLibNum == nil {
			data, err := obf.Data(ctx, m.DownloadOneBlockFile)
			if err != nil {
				return fmt.Errorf("getting one block file data %q: %w", filename, err)
			}

			blockReader, err := m.blockReaderFactory.New(bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("unable to read one block %q: %w", filename, err)
			}

			block, err := blockReader.Read()
			if block == nil {
				return err
			}

			obf.InnerLibNum = &block.LibNum
		}

		out = append(out, obf)

		return nil
	})

	return
}
