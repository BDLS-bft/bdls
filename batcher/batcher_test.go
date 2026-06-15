package batcher

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/BDLS-bft/bdls"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestBatcher_MaxCount(t *testing.T) {
	var batches [][]byte
	var mu sync.Mutex

	onBatch := func(b []byte) error {
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		return nil
	}

	// MaxCount = 3
	b := NewBatcher(0, 3, 0, onBatch)
	defer b.Close()

	err := b.Submit([]byte("tx1"))
	assert.NoError(t, err)
	err = b.Submit([]byte("tx2"))
	assert.NoError(t, err)

	mu.Lock()
	assert.Len(t, batches, 0) // not cut yet
	mu.Unlock()

	err = b.Submit([]byte("tx3"))
	assert.NoError(t, err)

	mu.Lock()
	assert.Len(t, batches, 1) // cut on 3rd submit
	var batch Batch
	err = proto.Unmarshal(batches[0], &batch)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("tx1"), []byte("tx2"), []byte("tx3")}, batch.Transactions)
	mu.Unlock()
}

func TestBatcher_MaxBytes(t *testing.T) {
	var batches [][]byte
	var mu sync.Mutex

	onBatch := func(b []byte) error {
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		return nil
	}

	// Estimate size:
	// A batch with two 100-byte txs will have protobuf overhead:
	// Field 1 tag: 1 byte
	// Length prefix: 1 byte (since length 100 < 128)
	// Tx data: 100 bytes
	// So ~102 bytes per tx.
	// We set MaxBytes to 200 bytes, which fits 1 tx but not 2.
	b := NewBatcher(200, 0, 0, onBatch)
	defer b.Close()

	tx := make([]byte, 100)

	err := b.Submit(tx)
	assert.NoError(t, err)

	mu.Lock()
	assert.Len(t, batches, 0)
	mu.Unlock()

	// This submit should exceed MaxBytes, causing the first tx to be cut.
	err = b.Submit(tx)
	assert.NoError(t, err)

	mu.Lock()
	assert.Len(t, batches, 1)
	var batch Batch
	err = proto.Unmarshal(batches[0], &batch)
	assert.NoError(t, err)
	assert.Len(t, batch.Transactions, 1)
	mu.Unlock()
}

func TestBatcher_MaxWait(t *testing.T) {
	var batches [][]byte
	var mu sync.Mutex
	done := make(chan struct{})

	onBatch := func(b []byte) error {
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		close(done)
		return nil
	}

	b := NewBatcher(0, 0, 50*time.Millisecond, onBatch)
	defer b.Close()

	err := b.Submit([]byte("tx"))
	assert.NoError(t, err)

	select {
	case <-done:
		mu.Lock()
		assert.Len(t, batches, 1)
		var batch Batch
		err = proto.Unmarshal(batches[0], &batch)
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("tx")}, batch.Transactions)
		mu.Unlock()
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for batch cut")
	}
}

func TestBatcher_Drain(t *testing.T) {
	b := NewBatcher(0, 0, 0, nil)
	defer b.Close()

	err := b.Submit([]byte("tx1"))
	assert.NoError(t, err)
	err = b.Submit([]byte("tx2"))
	assert.NoError(t, err)

	bytes, err := b.Drain()
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	var batch Batch
	err = proto.Unmarshal(bytes, &batch)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("tx1"), []byte("tx2")}, batch.Transactions)

	// Second drain should be empty
	bytes, err = b.Drain()
	assert.NoError(t, err)
	assert.Nil(t, bytes)
}

func TestBatcher_Concurrency(t *testing.T) {
	var batches [][]byte
	var mu sync.Mutex

	onBatch := func(b []byte) error {
		mu.Lock()
		batches = append(batches, b)
		mu.Unlock()
		return nil
	}

	b := NewBatcher(0, 10, 10*time.Millisecond, onBatch)
	defer b.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = b.Submit([]byte("concurrent-tx"))
			}
		}()
	}
	wg.Wait()

	// Drain any remaining
	rem, err := b.Drain()
	assert.NoError(t, err)
	if len(rem) > 0 {
		_ = onBatch(rem)
	}

	mu.Lock()
	defer mu.Unlock()
	totalTxs := 0
	for _, batchBytes := range batches {
		var batch Batch
		err := proto.Unmarshal(batchBytes, &batch)
		assert.NoError(t, err)
		totalTxs += len(batch.Transactions)
	}
	assert.Equal(t, 100, totalTxs)
}

func TestBatcher_ConsensusIntegration(t *testing.T) {
	keys := make([]*ecdsa.PrivateKey, 4)
	ids := make([]bdls.Identity, 4)
	for i := 0; i < 4; i++ {
		k, err := ecdsa.GenerateKey(bdls.S256Curve, rand.Reader)
		assert.NoError(t, err)
		keys[i] = k
		ids[i] = bdls.DefaultPubKeyToIdentity(&k.PublicKey)
	}

	cfg := &bdls.Config{
		Epoch:         time.Now(),
		CurrentHeight: 0,
		PrivateKey:    keys[0],
		Participants:  ids,
		StateCompare:  func(a bdls.State, b bdls.State) int { return bytes.Compare(a, b) },
		StateValidate: func(bdls.State) bool { return true },
	}
	c, err := bdls.NewConsensus(cfg)
	assert.NoError(t, err)

	var proposedBatches [][]byte
	onBatch := func(batch []byte) error {
		proposedBatches = append(proposedBatches, batch)
		c.Propose(batch)
		return nil
	}

	batcher := NewBatcher(0, 2, 0, onBatch)
	defer batcher.Close()

	err = batcher.Submit([]byte("tx1"))
	assert.NoError(t, err)
	err = batcher.Submit([]byte("tx2"))
	assert.NoError(t, err)

	assert.Len(t, proposedBatches, 1)
	assert.True(t, c.HasProposed(proposedBatches[0]))
}

