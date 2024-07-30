// Copyright 2022 Matrix Origin
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
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_TombstoneMarshalUnMarshal(t *testing.T) {
	t.Run("only has row id", func(t *testing.T) {
		original := tombstoneDataV1{}
		original.typ = 1
		for idx := 0; idx < 100; idx++ {
			original.inMemTombstones = append(original.inMemTombstones, types.RandomRowid())
		}

		buf := new(bytes.Buffer)
		_, err := original.MarshalWithBuf(buf)
		require.NoError(t, err)

		copied := tombstoneDataV1{}
		require.NoError(t, copied.UnMarshal(buf.Bytes()))

		require.Equal(t, original.typ, copied.typ)
		require.Equal(t, original.inMemTombstones, copied.inMemTombstones)
	})

	t.Run("only has un committed delta loc", func(t *testing.T) {
		original := tombstoneDataV1{}
		original.typ = 1

		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_Blockid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_binary.ToType())

		for idx := 0; idx < 100; idx++ {
			rowId := types.RandomRowid()
			bid := *rowId.BorrowBlockID()
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], bid, false, common.DefaultAllocator))

			ts := types.TS(rowId[:12])
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], ts, false, common.DefaultAllocator))

			name := objectio.MockLocation(objectio.MockObjectName())
			require.NoError(t, vector.AppendBytes(bat.Vecs[2], name[:], false, common.DefaultAllocator))
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		original.uncommittedDeltaLocs = bat

		buf := new(bytes.Buffer)
		_, err := original.MarshalWithBuf(buf)
		require.NoError(t, err)

		copied := tombstoneDataV1{}
		copied.uncommittedDeltaLocs = batch.New(false, nil)

		require.NoError(t, copied.UnMarshal(buf.Bytes()))

		require.Equal(t, original.typ, copied.typ)

		for idx := 0; idx < 3; idx++ {
			require.Equal(t, original.uncommittedDeltaLocs.Vecs[idx].Length(),
				copied.uncommittedDeltaLocs.Vecs[idx].Length())

			for x := range original.uncommittedDeltaLocs.Vecs[idx].Length() {
				require.Equal(t, original.uncommittedDeltaLocs.Vecs[idx].GetRawBytesAt(x),
					copied.uncommittedDeltaLocs.Vecs[idx].GetRawBytesAt(x))
			}
		}
	})

	t.Run("has all fields", func(t *testing.T) {
		original := tombstoneDataV1{}
		original.typ = 1

		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_Blockid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_binary.ToType())

		for idx := 0; idx < 100; idx++ {
			rowId := types.RandomRowid()
			bid := *rowId.BorrowBlockID()
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], bid, false, common.DefaultAllocator))

			ts := types.TS(rowId[:12])
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], ts, false, common.DefaultAllocator))

			name := objectio.MockLocation(objectio.MockObjectName())
			require.NoError(t, vector.AppendBytes(bat.Vecs[2], name[:], false, common.DefaultAllocator))
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		original.uncommittedDeltaLocs = bat
		original.committedDeltalocs = bat

		buf := new(bytes.Buffer)
		_, err := original.MarshalWithBuf(buf)
		require.NoError(t, err)

		copied := tombstoneDataV1{}
		copied.uncommittedDeltaLocs = batch.New(false, nil)
		copied.committedDeltalocs = batch.New(false, nil)

		require.NoError(t, copied.UnMarshal(buf.Bytes()))

		require.Equal(t, original.typ, copied.typ)

		bats1 := []*batch.Batch{original.uncommittedDeltaLocs, original.committedDeltalocs}
		bats2 := []*batch.Batch{copied.uncommittedDeltaLocs, copied.committedDeltalocs}

		for y := 0; y < 2; y++ {
			for idx := 0; idx < 3; idx++ {
				require.Equal(t, bats1[y].Vecs[idx].Length(), bats2[y].Vecs[idx].Length())

				for x := range bats1[y].Vecs[idx].Length() {
					require.Equal(t, bats1[y].Vecs[idx].GetRawBytesAt(x), bats2[y].Vecs[idx].GetRawBytesAt(x))
				}
			}
		}

	})
}

//func TestRelationDataV1_MarshalAndUnMarshal(t *testing.T) {
//
//	objID := types.NewObjectid()
//	objName := objectio.BuildObjectNameWithObjectID(objID)
//
//	extent := objectio.NewExtent(0x1f, 0x2f, 0x3f, 0x4f)
//	delLoc := objectio.BuildLocation(objName, extent, 0, 0)
//	metaLoc := objectio.ObjectLocation(delLoc)
//	cts := types.BuildTSForTest(1, 1)
//
//	//var blkInfos []*objectio.BlockInfoInProgress
//	relData := buildRelationDataV1()
//	blkNum := 10
//	for i := 0; i < blkNum; i++ {
//		blkID := types.NewBlockidWithObjectID(objID, uint16(blkNum))
//		blkInfo := objectio.BlockInfoInProgress{
//			BlockID:      *blkID,
//			EntryState:   true,
//			Sorted:       false,
//			MetaLoc:      metaLoc,
//			CommitTs:     *cts,
//			PartitionNum: int16(i),
//		}
//		//blkInfos = append(blkInfos, &blkInfo)
//		relData.AppendBlockInfo(blkInfo)
//	}
//
//	tombstoner := &tombstoneDataV1{
//		typ: engine.TombstoneV1,
//	}
//	deletes := types.BuildTestRowid(1, 1)
//	tombstoner.inMemTombstones = append(tombstoner.inMemTombstones, deletes)
//	tombstoner.inMemTombstones = append(tombstoner.inMemTombstones, deletes)
//
//	tombstoner.uncommittedDeltaLocs = append(tombstoner.uncommittedDeltaLocs, delLoc)
//	tombstoner.uncommittedDeltaLocs = append(tombstoner.uncommittedDeltaLocs, delLoc)
//
//	tombstoner.committedDeltalocs = append(tombstoner.committedDeltalocs, delLoc)
//	tombstoner.committedDeltalocs = append(tombstoner.committedDeltalocs, delLoc)
//
//	tombstoner.commitTS = append(tombstoner.commitTS, *cts)
//	tombstoner.commitTS = append(tombstoner.commitTS, *cts)
//
//	relData.AttachTombstones(tombstoner)
//	buf := relData.MarshalToBytes()
//
//	newRelData, err := UnmarshalRelationData(buf)
//	require.Nil(t, err)
//
//	tomIsEqual := func(t1 *tombstoneDataV1, t2 *tombstoneDataV1) bool {
//		if t1.typ != t2.typ || len(t1.inMemTombstones) != len(t2.inMemTombstones) ||
//			len(t1.uncommittedDeltaLocs) != len(t2.uncommittedDeltaLocs) ||
//			len(t1.committedDeltalocs) != len(t2.committedDeltalocs) ||
//			len(t1.commitTS) != len(t2.commitTS) {
//			return false
//		}
//		for i := 0; i < len(t1.inMemTombstones); i++ {
//			if !t1.inMemTombstones[i].Equal(t2.inMemTombstones[i]) {
//				return false
//			}
//		}
//
//		for i := 0; i < len(t1.uncommittedDeltaLocs); i++ {
//			if !bytes.Equal(t1.uncommittedDeltaLocs[i], t2.uncommittedDeltaLocs[i]) {
//				return false
//			}
//		}
//
//		for i := 0; i < len(t1.committedDeltalocs); i++ {
//			if !bytes.Equal(t1.committedDeltalocs[i], t2.committedDeltalocs[i]) {
//				return false
//			}
//		}
//
//		for i := 0; i < len(t1.commitTS); i++ {
//			if !t1.commitTS[i].Equal(&t2.commitTS[i]) {
//				return false
//			}
//		}
//		return true
//	}
//
//	isEqual := func(rd1 *relationDataBlkInfoListV1, rd2 *relationDataBlkInfoListV1) bool {
//		if rd1.typ != rd2.typ || rd1.DataCnt() != rd2.DataCnt() ||
//			rd1.isEmpty != rd2.isEmpty || rd1.tombstoneTyp != rd2.tombstoneTyp {
//			return false
//		}
//		//for i := 0; i < len(rd1.blkList); i++ {
//		//	if !bytes.Equal(objectio.EncodeBlockInfoInProgress(*rd1.blkList[i]),
//		//		objectio.EncodeBlockInfoInProgress(*rd2.blkList[i])) {
//		//		return false
//		//	}
//		//}
//		if !bytes.Equal(*rd1.blklist, *rd2.blklist) {
//			return false
//		}
//
//		return tomIsEqual(rd1.tombstones.(*tombstoneDataV1),
//			rd2.tombstones.(*tombstoneDataV1))
//
//	}
//	require.True(t, isEqual(relData, newRelData.(*relationDataBlkInfoListV1)))
//
//}
