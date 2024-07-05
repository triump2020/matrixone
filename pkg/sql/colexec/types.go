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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ResultPos struct {
	Rel int32
	Pos int32
}

func NewResultPos(rel int32, pos int32) ResultPos {
	return ResultPos{Rel: rel, Pos: pos}
}

// ReceiveInfo used to spec which node,
// and which registers you need
type ReceiveInfo struct {
	// it's useless
	NodeAddr string
	Uuid     uuid.UUID
}

type Server struct {
	hakeeper      logservice.CNHAKeeperClient
	uuidCsChanMap UuidProcMap
	//txn's local segments.
	cnSegmentMap CnSegmentMap

	receivedRunningPipeline RunningPipelineMapForRemoteNode
}

// RunningPipelineMapForRemoteNode
// is a map to record which pipeline was built for a remote node.
// these pipelines will send data to a remote node,
// we record them for a better control for their lives.
type RunningPipelineMapForRemoteNode struct {
	sync.Mutex

	fromRpcClientToRunningPipeline map[rpcClientItem]*process.Process
}

type rpcClientItem struct {
	// connection.
	tcp morpc.ClientSession

	// stream id.
	id uint64
}

func (srv *Server) RecordRunningPipeline(session morpc.ClientSession, id uint64, proc *process.Process) (queryCancel bool) {
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	item := rpcClientItem{
		tcp: session,
		id:  id,
	}

	// CancelRunningPipeline was called before, this query has been canceled.
	if _, ok := srv.receivedRunningPipeline.fromRpcClientToRunningPipeline[item]; ok {
		delete(srv.receivedRunningPipeline.fromRpcClientToRunningPipeline, item)
		return true
	}

	srv.receivedRunningPipeline.fromRpcClientToRunningPipeline[item] = proc
	return false
}

func (srv *Server) CancelRunningPipeline(session morpc.ClientSession, id uint64) {
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	item := rpcClientItem{
		tcp: session,
		id:  id,
	}

	p, ok := srv.receivedRunningPipeline.fromRpcClientToRunningPipeline[item]
	if ok {
		p.Cancel()
		delete(srv.receivedRunningPipeline.fromRpcClientToRunningPipeline, item)
	} else {
		// indicate that this query was canceled. once anyone want to start this query, should just return.
		srv.receivedRunningPipeline.fromRpcClientToRunningPipeline[item] = nil
	}
}

func (srv *Server) RemoveRunningPipeline(session morpc.ClientSession, id uint64) {
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	item := rpcClientItem{
		tcp: session,
		id:  id,
	}

	delete(srv.receivedRunningPipeline.fromRpcClientToRunningPipeline, item)
}

type uuidProcMapItem struct {
	proc *process.Process
}

type UuidProcMap struct {
	sync.Mutex
	mp map[uuid.UUID]uuidProcMapItem
}

type CnSegmentMap struct {
	sync.Mutex
	// tag whether a segment is generated by this txn
	// segmentName => uuid + file number
	// 1.mp[segmentName] = 1 => txnWorkSpace
	// 2.mp[segmentName] = 2 => Cn Blcok
	mp map[objectio.Segmentid]int32
}

// ReceiverOperator need to receive batch from proc.Reg.MergeReceivers
type ReceiverOperator struct {
	proc *process.Process

	// parameter for Merge-Type receiver.
	// Merge-Type specifys the operator receive batch from all
	// regs or single reg.
	//
	// Merge/MergeGroup/MergeLimit ... are Merge-Type
	// while Join/Intersect/Minus ... are not
	aliveMergeReceiver int
	chs                []chan *process.RegisterMessage
	receiverListener   []reflect.SelectCase
}

const (
	DefaultBatchSize = 8192
)
