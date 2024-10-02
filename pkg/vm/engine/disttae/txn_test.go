// Copyright 2021 - 2024 Matrix Origin
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

package disttae

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

func Test_GetUncommittedS3Tombstone(t *testing.T) {
	var statsList []objectio.ObjectStats
	for i := 0; i < 3; i++ {
		row := types.RandomRowid()
		stats := objectio.NewObjectStatsWithObjectID(row.BorrowObjectID(), false, false, true)
		objectio.SetObjectStatsRowCnt(stats, uint32(10+i*10))
		statsList = append(statsList, *stats)
	}

	var tid uint64
	txn := &Transaction{
		tombstoneObjs: struct {
			sync.RWMutex
			data map[uint64][]objectio.ObjectStats
		}{data: make(map[uint64][]objectio.ObjectStats)},
	}
	txn.tombstoneObjs.data[tid] = []objectio.ObjectStats{statsList[0], statsList[1], statsList[2]}

	objectSlice := objectio.ObjectStatsSlice{}

	require.NoError(t, txn.getUncommittedS3Tombstone(tid, func(stats *objectio.ObjectStats) {
		objectSlice.Append(stats[:])
	}))
	require.Equal(t, len(statsList), objectSlice.Len())

	for i, ss := range txn.tombstoneObjs.data[tid] {
		require.Equal(t, ss[:], objectSlice.Get(i)[:])
	}

}
