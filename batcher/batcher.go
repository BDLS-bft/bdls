package batcher

import (
	"errors"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
)

var (
	ErrClosed = errors.New("batcher closed")
)

// Batcher groups individual transactions into serialized batches based on size, count, or time limits.
type Batcher struct {
	MaxBytes int
	MaxCount int
	MaxWait  time.Duration
	OnBatch  func(batch []byte) error

	mu     sync.Mutex
	txs    [][]byte
	timer  *time.Timer
	closed bool
}

// NewBatcher creates a new Batcher with the specified configurations.
func NewBatcher(maxBytes, maxCount int, maxWait time.Duration, onBatch func([]byte) error) *Batcher {
	return &Batcher{
		MaxBytes: maxBytes,
		MaxCount: maxCount,
		MaxWait:  maxWait,
		OnBatch:  onBatch,
	}
}

// Submit adds a transaction to the current batch. If a limit is reached, it triggers OnBatch.
func (b *Batcher) Submit(tx []byte) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}

	// Estimate size if we append the transaction
	tempBatch := &Batch{
		Transactions: append(b.txs, tx),
	}
	tempSize := proto.Size(tempBatch)

	// If there are existing transactions, check if adding the new transaction would exceed limits
	if len(b.txs) > 0 {
		if (b.MaxCount > 0 && len(b.txs) >= b.MaxCount) || (b.MaxBytes > 0 && tempSize > b.MaxBytes) {
			batchBytes, err := b.cutLocked()
			b.mu.Unlock()
			if err == nil && len(batchBytes) > 0 && b.OnBatch != nil {
				if cbErr := b.OnBatch(batchBytes); cbErr != nil {
					return cbErr
				}
			}
			b.mu.Lock()
			if b.closed {
				b.mu.Unlock()
				return ErrClosed
			}
		}
	}

	b.txs = append(b.txs, tx)

	// If this is the first transaction in the batch, start the timer
	if len(b.txs) == 1 && b.MaxWait > 0 {
		b.timer = time.AfterFunc(b.MaxWait, b.handleTimeout)
	}

	// Check if this new transaction alone triggers a cut
	tempBatch = &Batch{
		Transactions: b.txs,
	}
	if (b.MaxCount > 0 && len(b.txs) >= b.MaxCount) || (b.MaxBytes > 0 && proto.Size(tempBatch) >= b.MaxBytes) {
		batchBytes, err := b.cutLocked()
		b.mu.Unlock()
		if err == nil && len(batchBytes) > 0 && b.OnBatch != nil {
			return b.OnBatch(batchBytes)
		}
		return err
	}

	b.mu.Unlock()
	return nil
}

// Drain flushes and returns the current batch immediately.
func (b *Batcher) Drain() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	return b.cutLocked()
}

// Close stops the batcher and releases resources.
func (b *Batcher) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	b.txs = nil
	return nil
}

func (b *Batcher) handleTimeout() {
	b.mu.Lock()
	if b.closed || len(b.txs) == 0 {
		b.timer = nil
		b.mu.Unlock()
		return
	}
	batchBytes, err := b.cutLocked()
	b.timer = nil
	b.mu.Unlock()

	if err == nil && len(batchBytes) > 0 && b.OnBatch != nil {
		_ = b.OnBatch(batchBytes)
	}
}

func (b *Batcher) cutLocked() ([]byte, error) {
	if len(b.txs) == 0 {
		return nil, nil
	}
	batch := &Batch{
		Transactions: b.txs,
	}
	b.txs = nil
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	return proto.Marshal(batch)
}
