// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnbase

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (txn *Txn) rollback1PC(ctx context.Context) (err error) {
	if txn.IsReplay() {
		panic(moerr.NewTAERollbackNoCtx("1pc txn %s should not be called here", txn.String()))
	}
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive {
		return moerr.NewTAERollbackNoCtx("unexpected txn status : %s", txnif.TxnStrState(state))
	}

	txn.Add(1)
	err = txn.Mgr.OnOpTxn(&OpTxn{
		ctx: ctx,
		Txn: txn,
		Op:  OpRollback,
	})
	if err != nil {
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err, true)
	}
	txn.Wait()
	//txn.Status = txnif.TxnStatusRollbacked
	if err = txn.Mgr.DeleteTxn(txn.GetID()); err != nil {
		return
	}
	return txn.Err
}

// Return the first number n such that n*base >= 1<<64.
func cutoff64(base int) uint64 {
	if base < 2 {
		return 0
	}
	return (1<<64-1)/uint64(base) + 1
}

// parseUintBytes is like strconv.ParseUint, but using a []byte.
func parseUintBytes(s []byte, base int, bitSize int) (n uint64, err error) {
	var cutoff, maxVal uint64

	if bitSize == 0 {
		bitSize = int(strconv.IntSize)
	}

	s0 := s
	switch {
	case len(s) < 1:
		err = strconv.ErrSyntax
		goto Error

	case 2 <= base && base <= 36:
		// valid base; nothing to do

	case base == 0:
		// Look for octal, hex prefix.
		switch {
		case s[0] == '0' && len(s) > 1 && (s[1] == 'x' || s[1] == 'X'):
			base = 16
			s = s[2:]
			if len(s) < 1 {
				err = strconv.ErrSyntax
				goto Error
			}
		case s[0] == '0':
			base = 8
		default:
			base = 10
		}

	default:
		err = moerr.NewTAEErrorNoCtx("invalid base " + strconv.Itoa(base))
		goto Error
	}

	n = 0
	cutoff = cutoff64(base)
	maxVal = 1<<uint(bitSize) - 1

	for i := 0; i < len(s); i++ {
		var v byte
		d := s[i]
		switch {
		case '0' <= d && d <= '9':
			v = d - '0'
		case 'a' <= d && d <= 'z':
			v = d - 'a' + 10
		case 'A' <= d && d <= 'Z':
			v = d - 'A' + 10
		default:
			n = 0
			err = strconv.ErrSyntax
			goto Error
		}
		if int(v) >= base {
			n = 0
			err = strconv.ErrSyntax
			goto Error
		}

		if n >= cutoff {
			// n*base overflows
			n = 1<<64 - 1
			err = strconv.ErrRange
			goto Error
		}
		n *= uint64(base)

		n1 := n + uint64(v)
		if n1 < n || n1 > maxVal {
			// n+v overflows
			n = 1<<64 - 1
			err = strconv.ErrRange
			goto Error
		}
		n = n1
	}

	return n, nil

Error:
	return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
}

var littleBuf = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64)
		return &buf
	},
}

var goroutineSpace = []byte("goroutine ")

func curGoroutineID() uint64 {
	bp := littleBuf.Get().(*[]byte)
	defer littleBuf.Put(bp)
	b := *bp
	b = b[:runtime.Stack(b, false)]
	// Parse the 4707 out of "goroutine 4707 ["
	b = bytes.TrimPrefix(b, goroutineSpace)
	i := bytes.IndexByte(b, ' ')
	if i < 0 {
		panic(fmt.Sprintf("No space found in %q", b))
	}
	b = b[:i]
	n, err := parseUintBytes(b, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse goroutine ID out of %q: %v", b, err))
	}
	return n
}

func (txn *Txn) commit1PC(ctx context.Context, _ bool) (err error) {
	state := txn.GetTxnState(false)
	if state != txnif.TxnStateActive {
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAECommitNoCtx("invalid txn state %s", txnif.TxnStrState(state))
	}
	txn.Add(1)

	start := time.Now()
	if err = txn.Freeze(); err == nil {
		if time.Since(start) > time.Millisecond*100 {
			fmt.Printf("Freeze with long latency, duration:%f, debug:%s.\n",
				time.Since(start).Seconds(),
				hex.EncodeToString(txn.GetCtx()))
		}

		now := time.Now()
		txn.SetEnqueuePrepTime(now)

		err = txn.Mgr.OnOpTxn(&OpTxn{
			ctx: ctx,
			Txn: txn,
			Op:  OpCommit,
		})

		if time.Since(now) > time.Millisecond*100 {
			fmt.Printf("Enqueue preparing queue with long latency, duration:%f, debug:%s.\n",
				time.Since(now).Seconds(),
				hex.EncodeToString(txn.GetCtx()))
		}
	}

	// TxnManager is closed
	if err != nil {
		txn.SetError(err)
		txn.Lock()
		_ = txn.ToRollbackingLocked(txn.GetStartTS().Next())
		txn.Unlock()
		_ = txn.PrepareRollback()
		_ = txn.ApplyRollback()
		txn.DoneWithErr(err, true)
	}

	now := time.Now()
	txn.Wait()
	//if txn.Err == nil {
	//txn.Status = txnif.TxnStatusCommitted
	//}
	if err = txn.Mgr.DeleteTxn(txn.GetID()); err != nil {
		return
	}
	if time.Since(now) > time.Millisecond*1000 {
		fmt.Printf("Commit1PC: wait txn commit done with long latency,"+
			" duration:%f, txnid:%s, curGroutineID : %d, "+
			"DequeuePrepTime - EnquePrepTime = %f,"+
			"DequeuePrepWalTime - EnqueuePrepWalTime = %f,"+
			"DequeueFlushTime - EnqueueFlushTime = %f. \n",
			time.Since(now).Seconds(),
			hex.EncodeToString(txn.GetCtx()),
			curGoroutineID(),
			txn.GetDequeuePrepTime().Sub(txn.GetEnqueuePrepTime()).Seconds(),
			txn.GetDequeuePrepWalTime().Sub(txn.GetEnqueuePrepWalTime()).Seconds(),
			txn.GetDequeueFlushTime().Sub(txn.GetEnqueueFlushTime()).Seconds())
	}
	return txn.GetError()
}

func (txn *Txn) rollback2PC(ctx context.Context) (err error) {
	state := txn.GetTxnState(false)

	switch state {
	case txnif.TxnStateActive:
		txn.Add(1)
		err = txn.Mgr.OnOpTxn(&OpTxn{
			ctx: ctx,
			Txn: txn,
			Op:  OpRollback,
		})
		if err != nil {
			_ = txn.PrepareRollback()
			_ = txn.ApplyRollback()
			_ = txn.ToRollbacking(txn.GetStartTS())
			txn.DoneWithErr(err, true)
		}
		txn.Wait()

	case txnif.TxnStatePrepared:
		//Notice that at this moment, txn had already appended data into state machine, so
		// we can not just delete the AppendNode from the MVCCHandle, instead ,we should
		// set the state of the AppendNode to Abort to make reader perceive it .
		_ = txn.ApplyRollback()
		txn.DoneWithErr(nil, true)

	default:
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAERollbackNoCtx("unexpected txn status : %s", txnif.TxnStrState(state))
	}

	txn.Mgr.DeleteTxn(txn.GetID())

	return txn.GetError()
}

func (txn *Txn) commit2PC(inRecovery bool) (err error) {
	state := txn.GetTxnState(false)
	txn.Mgr.OnCommitTxn(txn)

	switch state {
	//It's a 2PC transaction running on Coordinator
	case txnif.TxnStateCommittingFinished:
		if err = txn.ApplyCommit(); err != nil {
			panic(err)
		}
		txn.DoneWithErr(nil, false)

		// Skip logging if in recovery
		if !inRecovery {
			//Append a committed log entry into log service asynchronously
			//     for checkpointing the committing log entry
			_, err = txn.LogTxnState(false)
			if err != nil {
				panic(err)
			}
		}

	//It's a 2PC transaction running on Participant.
	//Notice that Commit must be successful once the commit message arrives,
	//since Committing had succeed.
	case txnif.TxnStatePrepared:
		if err = txn.ApplyCommit(); err != nil {
			panic(err)
		}
		txn.DoneWithErr(nil, false)

		// Skip logging if in recovery
		if !inRecovery {
			//Append committed log entry ,and wait it synced.
			_, err = txn.LogTxnState(true)
			if err != nil {
				panic(err)
			}
		}

	default:
		logutil.Warnf("unexpected txn state : %s", txnif.TxnStrState(state))
		return moerr.NewTAECommitNoCtx("invalid txn state %s", txnif.TxnStrState(state))
	}
	txn.Mgr.DeleteTxn(txn.GetID())

	return txn.GetError()
}

func (txn *Txn) done1PCWithErr(err error) {
	txn.DoneCond.L.Lock()
	defer txn.DoneCond.L.Unlock()

	if err != nil {
		txn.ToUnknownLocked()
		txn.SetError(err)
	} else {
		if txn.State == txnif.TxnStatePreparing {
			if err := txn.ToCommittedLocked(); err != nil {
				txn.SetError(err)
			}
		} else {
			if err := txn.ToRollbackedLocked(); err != nil {
				txn.SetError(err)
			}
		}
	}
	txn.WaitGroup.Done()
	txn.DoneCond.Broadcast()
}

func (txn *Txn) done2PCWithErr(err error, isAbort bool) {
	txn.DoneCond.L.Lock()
	defer txn.DoneCond.L.Unlock()

	endOfTxn := true
	done := true

	if err != nil {
		txn.ToUnknownLocked()
		txn.SetError(err)
	} else {
		switch txn.State {
		case txnif.TxnStateRollbacking:
			if err = txn.ToRollbackedLocked(); err != nil {
				panic(err)
			}
		case txnif.TxnStatePreparing:
			endOfTxn = false
			if err = txn.ToPreparedLocked(); err != nil {
				panic(err)
			}
		case txnif.TxnStateCommittingFinished:
			done = false
			if err = txn.ToCommittedLocked(); err != nil {
				panic(err)
			}
		case txnif.TxnStatePrepared:
			done = false
			if isAbort {
				if err = txn.ToRollbackedLocked(); err != nil {
					panic(err)
				}
			} else {
				if err = txn.ToCommittedLocked(); err != nil {
					panic(err)
				}
			}
		}
	}
	if done {
		txn.WaitGroup.Done()
	}

	if endOfTxn {
		txn.DoneCond.Broadcast()
	}
}
