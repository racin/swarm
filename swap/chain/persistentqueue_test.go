package chain

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/swarm/state"
)

func TestNewPersistentQueue(t *testing.T) {
	store := state.NewInmemoryStore()
	defer store.Close()

	queue := NewPersistentQueue(store, "testq")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	count := 100

	var errout error
	keys := make([]string, count)
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			var value uint64
			key, err := queue.Next(ctx, &value)
			if err != nil {
				errout = fmt.Errorf("failed to get next item: %v", err)
				return
			}

			if key != keys[i] {
				errout = fmt.Errorf("keys don't match: got %v, expected %v", key, keys[i])
				return
			}

			if value != uint64(i) {
				errout = fmt.Errorf("values don't match: got %v, expected %v", value, i)
				return
			}

			batch := new(state.StoreBatch)
			queue.Delete(batch, key)
			err = store.WriteBatch(batch)
			if err != nil {
				errout = fmt.Errorf("could not write batch: %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			var value = uint64(i)
			batch := new(state.StoreBatch)
			key, trigger, err := queue.Queue(batch, value)
			keys[i] = key
			if err != nil {
				errout = fmt.Errorf("failed to queue item: %v", err)
				return
			}
			err = store.WriteBatch(batch)
			if err != nil {
				errout = fmt.Errorf("failed to write batch: %v", err)
				return
			}

			trigger()
		}
	}()

	wg.Wait()

	if errout != nil {
		t.Fatal(errout)
	}
}
