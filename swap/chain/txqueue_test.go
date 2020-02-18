package chain

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/backends"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethersphere/swarm/state"
	mock "github.com/ethersphere/swarm/swap/chain/mock"
	"github.com/ethersphere/swarm/testutil"
)

func init() {
	testutil.Init()
}

var (
	senderKey, _  = crypto.HexToECDSA("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd")
	senderAddress = crypto.PubkeyToAddress(senderKey.PublicKey)
)

var defaultBackend = backends.NewSimulatedBackend(core.GenesisAlloc{
	senderAddress: {Balance: big.NewInt(1000000000000000000)},
}, 8000000)

func newTestBackend() *mock.TestBackend {
	return mock.NewTestBackend(defaultBackend)
}

var TestRequestTypeID = TransactionRequestTypeID{
	Handler:     "test",
	RequestType: "TestRequest",
}

type TxSchedulerTester struct {
	lock    sync.Mutex
	chans   map[uint64]*TxSchedulerTesterRequest
	backend Backend
}

type TxSchedulerTesterRequest struct {
	ReceiptNotification      chan *TransactionReceiptNotification
	StateChangedNotification chan *TransactionStateChangedNotification
	hash                     common.Hash
}

func newTxSchedulerTester(backend Backend, txq TxScheduler) *TxSchedulerTester {
	t := &TxSchedulerTester{
		backend: backend,
		chans:   make(map[uint64]*TxSchedulerTesterRequest),
	}
	txq.SetHandlers(TestRequestTypeID, &TransactionRequestHandlers{
		Send: t.SendTransactionRequest,
		NotifyReceipt: func(ctx context.Context, id uint64, notification *TransactionReceiptNotification) error {
			select {
			case t.getRequest(id).ReceiptNotification <- notification:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		NotifyStateChanged: func(ctx context.Context, id uint64, notification *TransactionStateChangedNotification) error {
			select {
			case t.getRequest(id).StateChangedNotification <- notification:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return t
}

func (tc *TxSchedulerTester) getRequest(id uint64) *TxSchedulerTesterRequest {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	c, ok := tc.chans[id]
	if !ok {
		tc.chans[id] = &TxSchedulerTesterRequest{
			ReceiptNotification:      make(chan *TransactionReceiptNotification),
			StateChangedNotification: make(chan *TransactionStateChangedNotification),
		}
		return tc.chans[id]
	}
	return c
}

func (tc *TxSchedulerTester) expectStateChangedNotification(ctx context.Context, id uint64, oldState TransactionRequestState, newState TransactionRequestState) error {
	var notification *TransactionStateChangedNotification
	request := tc.getRequest(id)
	select {
	case notification = <-request.StateChangedNotification:
	case <-ctx.Done():
		return ctx.Err()
	}

	if notification.OldState != oldState {
		return fmt.Errorf("wrong old state. got %v, expected %v", notification.OldState, oldState)
	}

	if notification.NewState != newState {
		return fmt.Errorf("wrong new state. got %v, expected %v", notification.NewState, newState)
	}

	return nil
}

func (tc *TxSchedulerTester) expectReceiptNotification(ctx context.Context, id uint64) error {
	var notification *TransactionReceiptNotification
	request := tc.getRequest(id)
	select {
	case notification = <-request.ReceiptNotification:
	case <-ctx.Done():
		return ctx.Err()
	}

	receipt, err := tc.backend.TransactionReceipt(ctx, request.hash)
	if err != nil {
		return err
	}
	if receipt == nil {
		return errors.New("no receipt found for transaction")
	}

	if notification.Receipt.TxHash != request.hash {
		return fmt.Errorf("wrong old state. got %v, expected %v", notification.Receipt.TxHash, request.hash)
	}

	return nil
}

func (tc *TxSchedulerTester) SendTransactionRequest(id uint64, backend Backend, opts *bind.TransactOpts) (hash common.Hash, err error) {
	var nonce uint64
	if opts.Nonce == nil {
		nonce, err = backend.PendingNonceAt(opts.Context, opts.From)
		if err != nil {
			return common.Hash{}, err
		}
	} else {
		nonce = opts.Nonce.Uint64()
	}

	signed, err := opts.Signer(types.HomesteadSigner{}, opts.From, types.NewTransaction(nonce, common.Address{}, big.NewInt(0), 100000, big.NewInt(int64(10000000)), []byte{}))
	if err != nil {
		return common.Hash{}, err
	}
	err = backend.SendTransaction(opts.Context, signed)
	if err != nil {
		return common.Hash{}, err
	}
	tc.getRequest(id).hash = signed.Hash()
	return signed.Hash(), nil
}

func setupTxQueueTest(run bool) (*TxQueue, func()) {
	backend := newTestBackend()
	store := state.NewInmemoryStore()
	txq := NewTxQueue(store, "test", backend, senderKey)
	if run {
		txq.Start()
	}
	return txq, func() {
		if run {
			txq.Stop()
		}
		store.Close()
		backend.Close()
	}
}

func TestTxQueueScheduleRequest(t *testing.T) {
	txq, clean := setupTxQueueTest(false)
	defer clean()
	tc := newTxSchedulerTester(txq.backend, txq)

	testRequest := 100

	id, err := txq.ScheduleRequest(TestRequestTypeID, testRequest)
	if err != nil {
		t.Fatal(err)
	}

	if id != 1 {
		t.Fatal("expected id to be 1")
	}

	var testRequestRetrieved int
	err = txq.GetRequest(id, &testRequestRetrieved)
	if err != nil {
		t.Fatal(err)
	}
	if testRequest != testRequestRetrieved {
		t.Fatalf("got request %d, expected %d", testRequestRetrieved, testRequest)
	}

	txq.Start()
	defer txq.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err = tc.expectStateChangedNotification(ctx, id, TransactionQueued, TransactionPending); err != nil {
		t.Fatal(err)
	}

	if err = tc.expectStateChangedNotification(ctx, id, TransactionPending, TransactionConfirmed); err != nil {
		t.Fatal(err)
	}

	if err = tc.expectReceiptNotification(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestTxQueueManyRequests(t *testing.T) {
	txq, clean := setupTxQueueTest(true)
	defer clean()
	tc := newTxSchedulerTester(txq.backend, txq)

	var ids []uint64
	count := 500
	for i := 0; i < count; i++ {
		id, err := txq.ScheduleRequest(TestRequestTypeID, 5)
		if err != nil {
			t.Fatal(err)
		}

		ids = append(ids, id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, id := range ids {
		err := tc.expectStateChangedNotification(ctx, id, TransactionQueued, TransactionPending)
		if err != nil {
			t.Fatal(err)
		}
		err = tc.expectStateChangedNotification(ctx, id, TransactionPending, TransactionConfirmed)
		if err != nil {
			t.Fatal(err)
		}
		err = tc.expectReceiptNotification(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
	}
}
