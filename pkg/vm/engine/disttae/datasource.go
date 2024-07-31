// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"bytes"
	"context"
	"slices"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type blockDeltaInfo struct {
	//the commit ts of location.
	cts types.TS
	loc objectio.Location
}

type tombstoneDataWithDeltaLoc struct {
	mp  *mpool.MPool
	typ engine.TombstoneType
	//in memory tombstones
	inMemTombstones []types.Rowid

	//persisted tombstones
	// written by CN, one block maybe respond to multi deltaLocs.
	uncommittedDeltaLocs *batch.Batch // bid, cts, loc

	committedDeltalocs *batch.Batch // bid, cts, loc

	//For improve the performance, but don't need to marshal and unmarshal the follow fields.
	init      bool
	blk2RowID map[types.Blockid][]types.Rowid
	//TODO:: remove it
	rowIDs map[types.Rowid]struct{}

	blk2UncommitLoc map[types.Blockid][]objectio.Location
	blk2CommitLoc   map[types.Blockid]blockDeltaInfo
}

func buildDeltaLocTombstoneWithProc(p *process.Process) *tombstoneDataWithDeltaLoc {
	t := &tombstoneDataWithDeltaLoc{
		mp:                   p.GetMPool(),
		typ:                  engine.TombstoneWithDeltaLoc,
		committedDeltalocs:   batch.NewWithSize(3),
		uncommittedDeltaLocs: batch.NewWithSize(3),
	}
	//attrs := []string{
	//	catalog.BlockMeta_ID,
	//	catalog.BlockMeta_CommitTs,
	//	catalog.BlockMeta_DeltaLoc}
	t.committedDeltalocs.Vecs[0] = p.GetVector(types.T_Blockid.ToType())
	t.committedDeltalocs.Vecs[1] = p.GetVector(types.T_TS.ToType())
	t.committedDeltalocs.Vecs[2] = p.GetVector(types.T_text.ToType())

	t.uncommittedDeltaLocs.Vecs[0] = p.GetVector(types.T_Blockid.ToType())
	t.uncommittedDeltaLocs.Vecs[1] = p.GetVector(types.T_TS.ToType())
	t.uncommittedDeltaLocs.Vecs[2] = p.GetVector(types.T_text.ToType())

	return t
}

func buildTombstoneWithDeltaLoc() *tombstoneDataWithDeltaLoc {
	return &tombstoneDataWithDeltaLoc{
		typ: engine.TombstoneWithDeltaLoc,
	}
}

func (tomb *tombstoneDataWithDeltaLoc) Init() error {
	if !tomb.init {
		tomb.blk2CommitLoc = make(map[types.Blockid]blockDeltaInfo)
		tomb.blk2UncommitLoc = make(map[types.Blockid][]objectio.Location)
		tomb.blk2RowID = make(map[types.Blockid][]types.Rowid)
		tomb.rowIDs = make(map[types.Rowid]struct{})

		bids := vector.MustFixedCol[types.Blockid](tomb.committedDeltalocs.GetVector(0))
		cts := vector.MustFixedCol[types.TS](tomb.committedDeltalocs.GetVector(1))
		locs, area := vector.MustVarlenaRawData(tomb.committedDeltalocs.GetVector(2))

		for i, bid := range bids {
			loc, err := blockio.EncodeLocationFromString(locs[i].UnsafeGetString(area))
			if err != nil {
				return err
			}
			tomb.blk2CommitLoc[bid] = blockDeltaInfo{
				cts: cts[i],
				loc: loc,
			}
		}

		//for uncommitted delta locs
		bids = vector.MustFixedCol[types.Blockid](tomb.uncommittedDeltaLocs.GetVector(0))
		cts = vector.MustFixedCol[types.TS](tomb.uncommittedDeltaLocs.GetVector(1))
		locs, area = vector.MustVarlenaRawData(tomb.uncommittedDeltaLocs.GetVector(2))

		for i, bid := range bids {
			loc, err := blockio.EncodeLocationFromString(locs[i].UnsafeGetString(area))
			if err != nil {
				return err
			}
			tomb.blk2UncommitLoc[bid] = append(tomb.blk2UncommitLoc[bid], loc)
		}

		for _, row := range tomb.inMemTombstones {
			blkID, _ := row.Decode()
			tomb.blk2RowID[blkID] = append(tomb.blk2RowID[blkID], row)
			tomb.rowIDs[row] = struct{}{}
		}
		tomb.init = true
	}
	return nil
}

func (tomb *tombstoneDataWithDeltaLoc) HasTombstones() bool {
	if len(tomb.inMemTombstones) == 0 &&
		tomb.committedDeltalocs.RowCount() == 0 &&
		tomb.uncommittedDeltaLocs.RowCount() == 0 {
		return false
	}
	return true
}

func (tomb *tombstoneDataWithDeltaLoc) UnMarshal(buf []byte) error {

	tomb.typ = engine.TombstoneType(types.DecodeUint8(buf))
	buf = buf[1:]

	rowIDCnt := types.DecodeUint32(buf)
	buf = buf[4:]
	for i := 0; i < int(rowIDCnt); i++ {
		rowid := types.DecodeFixed[types.Rowid](buf[:types.RowidSize])
		tomb.inMemTombstones = append(tomb.inMemTombstones, rowid)
		buf = buf[types.RowidSize:]
	}

	sizeOfUncommit := types.DecodeUint32(buf)
	buf = buf[4:]

	bat := batch.NewWithSize(3)
	if err := bat.UnmarshalBinary(buf[:sizeOfUncommit]); err != nil {
		return err
	}
	tomb.uncommittedDeltaLocs = bat
	buf = buf[sizeOfUncommit:]

	sizeOfCommit := types.DecodeUint32(buf)
	buf = buf[4:]

	bat = batch.NewWithSize(3)
	if err := bat.UnmarshalBinary(buf[:sizeOfCommit]); err != nil {
		return err
	}
	tomb.committedDeltalocs = bat
	buf = buf[sizeOfCommit:]
	return nil
}

func (tomb *tombstoneDataWithDeltaLoc) MarshalWithBuf(w *bytes.Buffer) (uint32, error) {
	var size uint32
	typ := uint8(tomb.typ)
	if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
		return 0, err
	}
	size += 1

	length := uint32(len(tomb.inMemTombstones))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return 0, err
	}
	size += 4

	for _, row := range tomb.inMemTombstones {
		if _, err := w.Write(types.EncodeFixed(row)); err != nil {
			return 0, err
		}
		size += types.RowidSize
	}

	var sizeOfbat1 uint32
	pos1 := size
	if _, err := w.Write(types.EncodeUint32(&sizeOfbat1)); err != nil {
		return 0, err
	}
	size += 4

	bytes, err := tomb.uncommittedDeltaLocs.MarshalBinary()
	if err != nil {
		return 0, err
	}
	sizeOfbat1 = uint32(len(bytes))
	//update the size of uncommittedDeltaLocs.
	copy(w.Bytes()[pos1:pos1+4], types.EncodeUint32(&sizeOfbat1))

	if _, err := w.Write(bytes); err != nil {
		return 0, err
	}
	size += sizeOfbat1

	var sizeOfbat2 uint32
	pos2 := size
	if _, err := w.Write(types.EncodeUint32(&sizeOfbat2)); err != nil {
		return 0, err
	}
	size += 4

	bytes, err = tomb.committedDeltalocs.MarshalBinary()
	if err != nil {
		return 0, err
	}
	sizeOfbat2 = uint32(len(bytes))
	//update the size of committedDeltaLocs.
	copy(w.Bytes()[pos2:pos2+4], types.EncodeUint32(&sizeOfbat2))

	if _, err := w.Write(bytes); err != nil {
		return 0, err
	}
	size += sizeOfbat2
	return size, nil
}

func (tomb *tombstoneDataWithDeltaLoc) ApplyInMemTombstones(
	bid types.Blockid, rowsOffset []int32,
) (left []int32, deleted []int64) {

	left = rowsOffset

	if rowIDs, ok := tomb.blk2RowID[bid]; ok {
		for _, row := range rowIDs {
			_, o := row.Decode()
			left, deleted = fastApplyDeletedRows(left, deleted, o)
		}
	}

	return
}

func (tomb *tombstoneDataWithDeltaLoc) ApplyPersistedTombstones(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int32,
	applyCommit func(
		ctx2 context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int32,
		left *[]int32,
		deleted *[]int64) (err error),
	applyUncommit func(
		ctx2 context.Context,
		loc objectio.Location,
		rowsOffset []int32,
		left *[]int32,
		deleted *[]int64) (err error),
) (left []int32, deleted []int64, err error) {

	if locs, ok := tomb.blk2UncommitLoc[bid]; ok {
		for _, loc := range locs {
			err = applyUncommit(ctx, loc, rowsOffset, &left, &deleted)
			if err != nil {
				return
			}
		}
	}

	if loc, ok := tomb.blk2CommitLoc[bid]; ok {
		err = applyCommit(ctx, loc.loc, loc.cts, rowsOffset, &left, &deleted)
		if err != nil {
			return
		}
	}
	return
}

func rowIdsToOffset(rowIds []types.Rowid, wantedType any) any {
	switch wantedType.(type) {
	case int32:
		var ret []int32
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, int32(offset))
		}
		return ret

	case uint32:
		var ret []uint32
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, uint32(offset))
		}
		return ret

	case uint64:
		var ret []uint64
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, uint64(offset))
		}
		return ret

	case int64:
		var ret []int64
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, int64(offset))
		}
		return ret
	}

	return nil
}

func (tomb *tombstoneDataWithDeltaLoc) Type() engine.TombstoneType {
	return tomb.typ
}

func (tomb *tombstoneDataWithDeltaLoc) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneDataWithDeltaLoc); ok {
		tomb.inMemTombstones = append(tomb.inMemTombstones, v.inMemTombstones...)

		for i, v := range v.committedDeltalocs.Vecs {
			if err := tomb.committedDeltalocs.Vecs[i].UnionBatch(
				v, 0, v.Length(), nil, tomb.mp); err != nil {
				return err
			}
		}

		for i, v := range v.uncommittedDeltaLocs.Vecs {
			if err := tomb.uncommittedDeltaLocs.Vecs[i].UnionBatch(
				v, 0, v.Length(), nil, tomb.mp); err != nil {
				return err
			}
		}

	}
	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
}

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := engine.RelDataType(data[0])
	switch typ {
	case engine.RelDataBlockList:
		rd1 := buildRelationDataV1()
		if err := rd1.UnMarshal(data); err != nil {
			return nil, err
		}
		return rd1, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
}

var _ engine.RelData = new(blockListRelData)

type blockListRelData struct {
	proc *process.Process
	typ  engine.RelDataType
	//blkList[0] is a empty block info
	//TODO:: change it to []byte, instead of *[]byte
	blklist *objectio.BlockInfoSliceInProgress

	//marshal tombstones if hasTombstones is true, otherwise don't need to marshal tombstones
	hasTombstones bool
	tombstoneTyp  engine.TombstoneType
	//tombstones
	tombstones engine.Tombstoner
}

func buildRelationDataV1() *blockListRelData {
	return &blockListRelData{
		typ:     engine.RelDataBlockList,
		blklist: &objectio.BlockInfoSliceInProgress{},
	}
}

func (relData *blockListRelData) GetShardIDList() []uint64 {
	panic("not supported")
}
func (relData *blockListRelData) GetShardID(i int) uint64 {
	panic("not supported")
}
func (relData *blockListRelData) SetShardID(i int, id uint64) {
	panic("not supported")
}
func (relData *blockListRelData) AppendShardID(id uint64) {
	panic("not supported")
}

func (relData *blockListRelData) GetBlockInfoSlice() objectio.BlockInfoSliceInProgress {
	return relData.blklist.GetAllBytes()
}

func (relData *blockListRelData) GetBlockInfo(i int) objectio.BlockInfoInProgress {
	return *relData.blklist.Get(i)
}

func (relData *blockListRelData) SetBlockInfo(i int, blk objectio.BlockInfoInProgress) {
	relData.blklist.Set(i, &blk)
}

func (relData *blockListRelData) AppendBlockInfo(blk objectio.BlockInfoInProgress) {
	relData.blklist.AppendBlockInfo(blk)
}

func (relData *blockListRelData) UnMarshal(data []byte) error {
	data = data[1:]

	sizeofblks := types.DecodeUint32(data)
	data = data[4:]

	*relData.blklist = data[:sizeofblks]
	data = data[sizeofblks:]

	hasTombstones := types.DecodeBool(data)
	relData.hasTombstones = hasTombstones
	data = data[1:]

	if hasTombstones {
		tombstoneTyp := engine.TombstoneType(types.DecodeUint8(data))
		relData.tombstoneTyp = tombstoneTyp
		data = data[1:]

		size := types.DecodeUint32(data)
		data = data[4:]
		switch tombstoneTyp {
		case engine.TombstoneWithDeltaLoc:
			tombstoner := buildTombstoneWithDeltaLoc()
			if err := tombstoner.UnMarshal(data[:size]); err != nil {
				return err
			}
			relData.AttachTombstones(tombstoner)
		default:
			return moerr.NewInternalErrorNoCtx("unsupported tombstone type")
		}
	}

	return nil
}

func (relData *blockListRelData) MarshalWithBuf(w *bytes.Buffer) error {
	var pos2 uint32
	typ := uint8(relData.typ)
	if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
		return err
	}
	pos2 += 1

	sizeofblks := uint32(relData.blklist.Size())
	if _, err := w.Write(types.EncodeUint32(&sizeofblks)); err != nil {
		return err
	}
	pos2 += 4

	//marshal blk list
	if _, err := w.Write(*relData.blklist); err != nil {
		return err
	}
	pos2 += sizeofblks

	if _, err := w.Write(types.EncodeBool(&relData.hasTombstones)); err != nil {
		return err
	}
	pos2 += 1

	if relData.hasTombstones {
		typ := uint8(relData.tombstoneTyp)
		if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
			return err
		}
		pos2 += 1

		var sizeOfTombstones uint32
		// reserve the space: 4 bytes for size of tombstones.
		if _, err := w.Write(types.EncodeUint32(&sizeOfTombstones)); err != nil {
			return err
		}

		space, err := relData.tombstones.MarshalWithBuf(w)
		if err != nil {
			return err
		}
		//update the size of tombstones.
		copy(w.Bytes()[pos2:pos2+4], types.EncodeUint32(&space))

	}
	return nil
}

func (relData *blockListRelData) GetType() engine.RelDataType {
	return relData.typ
}

func (relData *blockListRelData) MarshalToBytes() []byte {
	var w bytes.Buffer
	if err := relData.MarshalWithBuf(&w); err != nil {
		return nil
	}
	buf := w.Bytes()
	return buf
}

func (relData *blockListRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	relData.tombstones = tombstones
	relData.tombstoneTyp = tombstones.Type()
	relData.hasTombstones = tombstones.HasTombstones()
	return nil
}

func (relData *blockListRelData) GetTombstones() engine.Tombstoner {
	return relData.tombstones
}

func (relData *blockListRelData) DataSlice(i, j int) engine.RelData {
	blist := objectio.BlockInfoSliceInProgress(relData.blklist.Slice(i, j))
	return &blockListRelData{
		typ:           relData.typ,
		blklist:       &blist,
		hasTombstones: relData.hasTombstones,
		tombstoneTyp:  relData.tombstoneTyp,
		tombstones:    relData.tombstones,
	}
}

func (relData *blockListRelData) GroupByPartitionNum() map[int16]engine.RelData {
	ret := make(map[int16]engine.RelData)

	blks := relData.GetBlockInfoSlice()
	blksLen := blks.Len()
	for idx := range blksLen {
		blkInfo := blks.Get(idx)
		if blkInfo.IsMemBlk() {
			return nil
		}
		partitionNum := blkInfo.PartitionNum
		if _, ok := ret[partitionNum]; !ok {
			ret[partitionNum] = &blockListRelData{
				typ:           relData.typ,
				hasTombstones: relData.hasTombstones,
				tombstoneTyp:  relData.tombstoneTyp,
				tombstones:    relData.tombstones,
			}
			ret[partitionNum].AppendBlockInfo(objectio.EmptyBlockInfoInProgress)
		}
		ret[partitionNum].AppendBlockInfo(*blkInfo)
	}

	return ret
}

func (relData *blockListRelData) BuildEmptyRelData() engine.RelData {
	return &blockListRelData{
		blklist: &objectio.BlockInfoSliceInProgress{},
		typ:     relData.typ,
	}
}

func (relData *blockListRelData) DataCnt() int {
	return relData.blklist.Len()
}

type RemoteDataSource struct {
	ctx  context.Context
	proc *process.Process

	fs fileservice.FileService
	ts types.TS

	cursor int
	data   engine.RelData
}

func NewRemoteDataSource(
	ctx context.Context,
	proc *process.Process,
	fs fileservice.FileService,
	snapshotTS timestamp.Timestamp,
	relData engine.RelData,
) (source *RemoteDataSource) {
	return &RemoteDataSource{
		data: relData,
		ctx:  ctx,
		proc: proc,
		fs:   fs,
		ts:   types.TimestampToTS(snapshotTS),
	}
}

func (rs *RemoteDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ any,
	_ *mpool.MPool,
	_ engine.VectorPool,
	_ *batch.Batch) (*objectio.BlockInfoInProgress, engine.DataState, error) {

	if rs.cursor >= rs.data.DataCnt() {
		return nil, engine.End, nil
	}
	rs.cursor++
	cur := rs.data.GetBlockInfo(rs.cursor - 1)
	return &cur, engine.Persisted, nil
}

func (rs *RemoteDataSource) Close() {

}

func (rs *RemoteDataSource) applyInMemTombstones(
	bid types.Blockid,
	rowsOffset []int32,
) (leftRows []int32, deletedRows []int64) {
	if rs.data.GetTombstones() == nil {
		return rowsOffset, nil
	}
	return rs.data.GetTombstones().ApplyInMemTombstones(
		bid,
		rowsOffset)
}

func (rs *RemoteDataSource) applyPersistedTombstones(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int32,
) (leftRows []int32, deletedRows []int64, err error) {

	applyUncommit := func(
		ctx context.Context,
		loc objectio.Location,
		rowsOffset []int32,
		left *[]int32,
		deleted *[]int64) error {
		rowIdBat, release, err := blockio.LoadTombstoneColumns(
			ctx,
			[]uint16{0},
			nil,
			rs.fs,
			loc,
			rs.proc.GetMPool())
		if err != nil {
			return err
		}
		defer release()

		*left = rowsOffset
		deletes := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))

		for _, delete := range deletes {
			o := delete.GetRowOffset()
			*left, *deleted = fastApplyDeletedRows(*left, *deleted, o)
		}
		return nil
	}

	applyCommit := func(
		ctx context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int32,
		left *[]int32,
		deleted *[]int64) error {
		deletes, err := loadBlockDeletesByDeltaLoc(ctx, rs.fs, bid, loc, rs.ts, cts)
		if err != nil {
			return err
		}

		if rowsOffset != nil {
			for _, offset := range rowsOffset {
				if deletes.Contains(uint64(offset)) {
					continue
				}
				*left = append(*left, offset)
			}
		} else {
			deletes.Foreach(func(u uint64) bool {
				*deleted = append(*deleted, int64(u))
				return true
			})
		}
		return nil
	}

	if rs.data.GetTombstones() == nil {
		return rowsOffset, nil, nil
	}
	return rs.data.GetTombstones().ApplyPersistedTombstones(
		ctx,
		bid,
		rowsOffset,
		applyCommit,
		applyUncommit)
}

func (rs *RemoteDataSource) ApplyTombstonesInProgress(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int32,
) ([]int32, error) {

	slices.SortFunc(rowsOffset, func(a, b int32) int {
		return int(a - b)
	})

	var err error
	rowsOffset, _ = rs.applyInMemTombstones(bid, rowsOffset)
	rowsOffset, _, err = rs.applyPersistedTombstones(ctx, bid, rowsOffset)
	if err != nil {
		return nil, err
	}
	return rowsOffset, nil
}

func (rs *RemoteDataSource) GetTombstonesInProgress(
	ctx context.Context, bid objectio.Blockid,
) (deletedRows []int64, err error) {

	_, dels := rs.applyInMemTombstones(bid, nil)
	deletedRows = append(deletedRows, dels...)

	_, dels, err = rs.applyPersistedTombstones(ctx, bid, nil)
	if err != nil {
		return
	}
	deletedRows = append(deletedRows, dels...)

	return
}

func (rs *RemoteDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	panic("Not Support order by")
}

func (rs *RemoteDataSource) GetOrderBy() []*plan.OrderBySpec {
	panic("Not Support order by")
}

func (rs *RemoteDataSource) SetFilterZM(zm objectio.ZoneMap) {
	panic("Not Support order by")
}

// local data source

type LocalDataSource struct {
	ranges []*objectio.BlockInfoInProgress
	pState *logtailreplay.PartitionState

	memPKFilter *MemPKFilter
	pStateRows  struct {
		insIter logtailreplay.RowsIter
	}

	table     *txnTable
	wsCursor  int
	txnOffset int

	// runtime config
	rc struct {
		batchPrefetchCursor int
		WorkspaceLocked     bool
		SkipPStateDeletes   bool
	}

	mp  *mpool.MPool
	ctx context.Context
	fs  fileservice.FileService

	rangesCursor int
	snapshotTS   types.TS
	iteratePhase engine.DataState

	//TODO:: It's so ugly, need to refactor
	//for order by
	desc     bool
	blockZMS []index.ZM
	sorted   bool // blks need to be sorted by zonemap
	OrderBy  []*plan.OrderBySpec

	filterZM objectio.ZoneMap
}

func NewLocalDataSource(
	ctx context.Context,
	table *txnTable,
	txnOffset int,
	rangesSlice objectio.BlockInfoSliceInProgress,
	skipReadMem bool,
) (source *LocalDataSource, err error) {

	source = &LocalDataSource{}
	source.fs = table.getTxn().engine.fs
	source.ctx = ctx
	source.mp = table.proc.Load().Mp()

	if bytes.Equal(
		objectio.EncodeBlockInfoInProgress(*rangesSlice.Get(0)),
		objectio.EmptyBlockInfoInProgressBytes) {
		rangesSlice = rangesSlice.Slice(1, rangesSlice.Len())
	}

	rangeLen := rangesSlice.Len()
	for i := 0; i < rangeLen; i++ {
		source.ranges = append(source.ranges, rangesSlice.Get(i))
	}

	state, err := table.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	source.table = table
	source.pState = state
	source.txnOffset = txnOffset
	source.snapshotTS = types.TimestampToTS(table.getTxn().op.SnapshotTS())

	source.iteratePhase = engine.InMem
	if skipReadMem {
		source.iteratePhase = engine.Persisted
	}

	return source, nil
}

func (ls *LocalDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	ls.OrderBy = orderby
}

func (ls *LocalDataSource) GetOrderBy() []*plan.OrderBySpec {
	return ls.OrderBy
}

func (ls *LocalDataSource) SetFilterZM(zm objectio.ZoneMap) {
	if !ls.filterZM.IsInited() {
		ls.filterZM = zm.Clone()
		return
	}
	if ls.desc && ls.filterZM.CompareMax(zm) < 0 {
		ls.filterZM = zm.Clone()
		return
	}
	if !ls.desc && ls.filterZM.CompareMin(zm) > 0 {
		ls.filterZM = zm.Clone()
		return
	}
}

func (ls *LocalDataSource) needReadBlkByZM(i int) bool {
	zm := ls.blockZMS[i]
	if !ls.filterZM.IsInited() || !zm.IsInited() {
		return true
	}
	if ls.desc {
		return ls.filterZM.CompareMax(zm) <= 0
	} else {
		return ls.filterZM.CompareMin(zm) >= 0
	}
}

func (ls *LocalDataSource) getBlockZMs() {
	orderByCol, _ := ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col)

	def := ls.table.tableDef
	orderByColIDX := int(def.Cols[int(orderByCol.Col.ColPos)].Seqnum)

	ls.blockZMS = make([]index.ZM, len(ls.ranges))
	var objDataMeta objectio.ObjectDataMeta
	var location objectio.Location
	for i := range ls.ranges {
		location = ls.ranges[i].MetaLocation()
		if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
			objMeta, err := objectio.FastLoadObjectMeta(ls.ctx, &location, false, ls.fs)
			if err != nil {
				panic("load object meta error when ordered scan!")
			}
			objDataMeta = objMeta.MustDataMeta()
		}
		blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
		ls.blockZMS[i] = blkMeta.ColumnMeta(uint16(orderByColIDX)).ZoneMap()
	}
}

func (ls *LocalDataSource) sortBlockList() {
	helper := make([]*blockSortHelperInProgress, len(ls.ranges))
	for i := range ls.ranges {
		helper[i] = &blockSortHelperInProgress{}
		helper[i].blk = ls.ranges[i]
		helper[i].zm = ls.blockZMS[i]
	}
	if ls.desc {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMax(zm2) > 0
		})
	} else {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMin(zm2) < 0
		})
	}

	for i := range helper {
		ls.ranges[i] = helper[i].blk
		ls.blockZMS[i] = helper[i].zm
	}
}

func (ls *LocalDataSource) deleteFirstNBlocks(n int) {
	ls.ranges = ls.ranges[n:]
	if len(ls.OrderBy) > 0 {
		ls.blockZMS = ls.blockZMS[n:]
	}
}

func (ls *LocalDataSource) HasTombstones(bid types.Blockid) bool {
	panic("implement me")
}

func (ls *LocalDataSource) Close() {
	ls.pStateRows.insIter.Close()
}

func (ls *LocalDataSource) ApplyTombstones(rows []types.Rowid) (sel []int64, err error) {
	panic("implement me")
}

func (ls *LocalDataSource) Next(
	ctx context.Context,
	cols []string,
	types []types.Type,
	seqNums []uint16,
	filter any,
	mp *mpool.MPool,
	vp engine.VectorPool,
	bat *batch.Batch,
) (*objectio.BlockInfoInProgress, engine.DataState, error) {

	if ls.memPKFilter == nil {
		ff := filter.(MemPKFilter)
		ls.memPKFilter = &ff
	}

	if len(cols) == 0 {
		return nil, engine.End, nil
	}

	// bathed prefetch block data and deletes
	ls.batchPrefetch(seqNums)

	for {
		switch ls.iteratePhase {
		case engine.InMem:
			err := ls.iterateInMemData(ctx, cols, types, seqNums, bat, mp, vp)
			if err != nil {
				return nil, engine.InMem, err
			}

			if bat.RowCount() == 0 {
				ls.iteratePhase = engine.Persisted
				continue
			}

			return nil, engine.InMem, nil

		case engine.Persisted:
			if len(ls.ranges) == 0 {
				return nil, engine.End, nil
			}

			ls.handleOrderBy()

			if len(ls.ranges) == 0 {
				return nil, engine.End, nil
			}

			blk := ls.ranges[0]
			ls.deleteFirstNBlocks(1)

			return blk, engine.Persisted, nil

		case engine.End:
			return nil, ls.iteratePhase, nil
		}
	}
}

func (ls *LocalDataSource) handleOrderBy() {
	// for ordered scan, sort blocklist by zonemap info, and then filter by zonemap
	if len(ls.OrderBy) > 0 {
		if !ls.sorted {
			ls.desc = ls.OrderBy[0].Flag&plan.OrderBySpec_DESC != 0
			ls.getBlockZMs()
			ls.sortBlockList()
			ls.sorted = true
		}
		i := 0
		for i < len(ls.ranges) {
			if ls.needReadBlkByZM(i) {
				break
			}
			i++
		}
		ls.deleteFirstNBlocks(i)

		if ls.table.tableName == "statement_info" {
			logutil.Infof("xxxx txn:%s, handle order by,delete blks:%d, rest blks:%d",
				ls.table.db.op.Txn().DebugString(),
				i,
				len(ls.ranges))
		}
	}
}

func (ls *LocalDataSource) iterateInMemData(
	ctx context.Context,
	cols []string,
	colTypes []types.Type,
	seqNums []uint16,
	bat *batch.Batch,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (err error) {

	bat.SetRowCount(0)

	if err = ls.filterInMemUnCommittedInserts(seqNums, mp, bat); err != nil {
		return err
	}

	if err = ls.filterInMemCommittedInserts(colTypes, seqNums, mp, bat); err != nil {
		return err
	}

	return nil
}

func checkWorkspaceEntryType(tbl *txnTable, entry Entry) int {
	if entry.DatabaseId() != tbl.db.databaseId || entry.TableId() != tbl.tableId {
		return -1
	}

	if entry.typ == INSERT || entry.typ == INSERT_TXN {
		if entry.bat == nil || entry.bat.IsEmpty() {
			return -1
		}
		if entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			return -1
		}

		return INSERT
	}

	if entry.IsGeneratedByTruncate() {
		return -1
	}

	if (entry.typ == DELETE || entry.typ == DELETE_TXN) && entry.fileName == "" {
		return DELETE
	}

	return -1
}

func (ls *LocalDataSource) filterInMemUnCommittedInserts(
	seqNums []uint16,
	mp *mpool.MPool,
	bat *batch.Batch,
) error {

	if ls.wsCursor >= ls.txnOffset {
		return nil
	}

	ls.table.getTxn().Lock()
	ls.rc.WorkspaceLocked = true
	defer func() {
		ls.table.getTxn().Unlock()
		ls.rc.WorkspaceLocked = false
	}()

	rows := 0
	writes := ls.table.getTxn().writes
	maxRows := int(options.DefaultBlockMaxRows)
	if len(writes) == 0 {
		return nil
	}

	for ; ls.wsCursor < ls.txnOffset &&
		rows+writes[ls.wsCursor].bat.RowCount() <= maxRows; ls.wsCursor++ {
		entry := ls.table.getTxn().writes[ls.wsCursor]

		if checkWorkspaceEntryType(ls.table, entry) != INSERT {
			continue
		}

		insRowIDs := vector.MustFixedCol[types.Rowid](entry.bat.Vecs[0])
		offsets := rowIdsToOffset(insRowIDs, int32(0)).([]int32)

		b, _ := insRowIDs[0].Decode()
		sels, err := ls.ApplyTombstonesInProgress(ls.ctx, b, offsets)
		if err != nil {
			return err
		}

		if len(sels) == 0 {
			continue
		}

		rows += len(sels)

		for i, destVec := range bat.Vecs {
			uf := vector.GetUnionOneFunction(*destVec.GetType(), mp)

			colIdx := int(seqNums[i])
			if colIdx != objectio.SEQNUM_ROWID {
				colIdx++
			} else {
				colIdx = 0
			}

			for j := range sels {
				if err = uf(destVec, entry.bat.Vecs[colIdx], int64(j)); err != nil {
					return err
				}
			}
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

func (ls *LocalDataSource) filterInMemCommittedInserts(
	colTypes []types.Type, seqNums []uint16, mp *mpool.MPool, bat *batch.Batch,
) error {

	// in meme committed insert only need to apply deletes that exists
	// in workspace and flushed to s3 but not commit.
	ls.rc.SkipPStateDeletes = true
	defer func() {
		ls.rc.SkipPStateDeletes = false
	}()

	if bat.RowCount() >= int(options.DefaultBlockMaxRows) {
		return nil
	}

	var (
		err          error
		sel          []int32
		appendedRows = bat.RowCount()
	)

	appendFunctions := make([]func(*vector.Vector, *vector.Vector, int64) error, len(bat.Attrs))
	for i := range bat.Attrs {
		appendFunctions[i] = vector.GetUnionOneFunction(colTypes[i], mp)
	}

	if ls.pStateRows.insIter == nil {
		if ls.memPKFilter.SpecFactory == nil {
			ls.pStateRows.insIter = ls.pState.NewRowsIter(ls.snapshotTS, nil, false)
		} else {
			ls.pStateRows.insIter = ls.pState.NewPrimaryKeyIter(
				ls.memPKFilter.TS, ls.memPKFilter.SpecFactory(ls.memPKFilter))
		}
	}

	for appendedRows < int(options.DefaultBlockMaxRows) && ls.pStateRows.insIter.Next() {
		entry := ls.pStateRows.insIter.Entry()
		b, o := entry.RowID.Decode()

		sel, err = ls.ApplyTombstonesInProgress(ls.ctx, b, []int32{int32(o)})
		if err != nil {
			return err
		}

		if len(sel) == 0 {
			continue
		}

		for i, name := range bat.Attrs {
			if name == catalog.Row_ID {
				if err = vector.AppendFixed(
					bat.Vecs[i],
					entry.RowID,
					false,
					mp); err != nil {
					return err
				}
			} else {
				idx := 2 /*rowid and commits*/ + seqNums[i]
				if int(idx) >= len(entry.Batch.Vecs) /*add column*/ ||
					entry.Batch.Attrs[idx] == "" /*drop column*/ {
					err = vector.AppendAny(
						bat.Vecs[i],
						nil,
						true,
						mp)
				} else {
					err = appendFunctions[i](
						bat.Vecs[i],
						entry.Batch.Vecs[int(2+seqNums[i])],
						entry.Offset,
					)
				}
				if err != nil {
					return err
				}
			}
		}
		appendedRows++
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func loadBlockDeletesByDeltaLoc(
	ctx context.Context,
	fs fileservice.FileService,
	blockId types.Blockid,
	deltaLoc objectio.Location,
	snapshotTS, blockCommitTS types.TS,
) (deleteMask *nulls.Nulls, err error) {

	var (
		rows *nulls.Nulls
		//bisect           time.Duration
		release          func()
		persistedByCN    bool
		persistedDeletes *batch.Batch
	)

	if !deltaLoc.IsEmpty() {
		//t1 := time.Now()

		if persistedDeletes, persistedByCN, release, err = blockio.ReadBlockDelete(ctx, deltaLoc, fs); err != nil {
			return nil, err
		}
		defer release()

		//readCost := time.Since(t1)

		if persistedByCN {
			rows = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(persistedDeletes, snapshotTS, blockCommitTS)
		} else {
			//t2 := time.Now()
			rows = blockio.EvalDeleteRowsByTimestamp(persistedDeletes, snapshotTS, &blockId)
			//bisect = time.Since(t2)
		}

		if rows != nil {
			deleteMask = rows
		}

		//readTotal := time.Since(t1)
		//blockio.RecordReadDel(readTotal, readCost, bisect)
	}

	return deleteMask, nil
}

// ApplyTombstonesInProgress check if any deletes exist in
//  1. unCommittedInmemDeletes:
//     a. workspace writes
//     b. flushed to s3
//     c. raw rowId offset deletes (not flush yet)
//  3. committedInmemDeletes
//  4. committedPersistedTombstone
func (ls *LocalDataSource) ApplyTombstonesInProgress(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int32,
) ([]int32, error) {

	slices.SortFunc(rowsOffset, func(a, b int32) int {
		return int(a - b)
	})

	var err error

	rowsOffset, _ = ls.applyWorkspaceEntryDeletes(bid, rowsOffset)
	rowsOffset, _, err = ls.applyWorkspaceFlushedS3Deletes(bid, rowsOffset)
	if err != nil {
		return nil, err
	}

	rowsOffset, _ = ls.applyWorkspaceRawRowIdDeletes(bid, rowsOffset)
	rowsOffset, _ = ls.applyPStateInMemDeletes(bid, rowsOffset)
	rowsOffset, _, err = ls.applyPStatePersistedDeltaLocation(bid, rowsOffset)
	if err != nil {
		return nil, err
	}

	return rowsOffset, nil
}

func (ls *LocalDataSource) GetTombstonesInProgress(
	ctx context.Context, bid objectio.Blockid,
) (deletedRows []int64, err error) {

	_, dels := ls.applyWorkspaceEntryDeletes(bid, nil)
	deletedRows = append(deletedRows, dels...)

	_, dels, err = ls.applyWorkspaceFlushedS3Deletes(bid, nil)
	if err != nil {
		return nil, err
	}
	deletedRows = append(deletedRows, dels...)

	_, dels = ls.applyWorkspaceRawRowIdDeletes(bid, nil)
	deletedRows = append(deletedRows, dels...)

	_, dels = ls.applyPStateInMemDeletes(bid, nil)
	deletedRows = append(deletedRows, dels...)

	_, dels, err = ls.applyPStatePersistedDeltaLocation(bid, nil)
	if err != nil {
		return nil, err
	}
	deletedRows = append(deletedRows, dels...)

	return deletedRows, nil
}

func fastApplyDeletedRows(
	leftRows []int32, deletedRows []int64, o uint32,
) ([]int32, []int64) {
	if len(leftRows) != 0 {
		if x, found := sort.Find(len(leftRows), func(i int) int {
			return int(int32(o) - leftRows[i])
		}); found {
			leftRows = append(leftRows[:x], leftRows[x+1:]...)
		}
	} else {
		deletedRows = append(deletedRows, int64(o))
	}

	return leftRows, deletedRows
}

func (ls *LocalDataSource) applyWorkspaceEntryDeletes(
	bid objectio.Blockid, offsets []int32,
) (leftRows []int32, deletedRows []int64) {

	leftRows = offsets

	// may have locked in `filterInMemUnCommittedInserts`
	if !ls.rc.WorkspaceLocked {
		ls.table.getTxn().Lock()
		defer ls.table.getTxn().Unlock()
	}

	done := false
	writes := ls.table.getTxn().writes[:ls.txnOffset]

	for idx := range writes {
		if checkWorkspaceEntryType(ls.table, writes[idx]) != DELETE {
			continue
		}

		delRowIds := vector.MustFixedCol[types.Rowid](writes[idx].bat.Vecs[0])
		for _, delRowId := range delRowIds {
			b, o := delRowId.Decode()
			if bid.Compare(b) != 0 {
				continue
			}

			leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, o)
			if leftRows != nil && len(leftRows) == 0 {
				done = true
				break
			}
		}

		if done {
			break
		}
	}

	return leftRows, deletedRows
}

func (ls *LocalDataSource) applyWorkspaceFlushedS3Deletes(
	bid objectio.Blockid, offsets []int32,
) (leftRows []int32, deletedRows []int64, err error) {

	leftRows = offsets

	s3FlushedDeletes := &ls.table.getTxn().blockId_tn_delete_metaLoc_batch
	s3FlushedDeletes.RWMutex.Lock()
	defer s3FlushedDeletes.RWMutex.Unlock()

	if len(s3FlushedDeletes.data[bid]) == 0 || ls.pState.BlockPersisted(bid) {
		return
	}

	done := false
	for _, bat := range s3FlushedDeletes.data[bid] {
		vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
		for i := range vs {
			location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
			if err != nil {
				return nil, nil, err
			}

			rowIdBat, release, err := blockio.LoadColumns(ls.ctx, []uint16{0}, nil, ls.fs, location, ls.mp, fileservice.Policy(0))
			if err != nil {
				release()
				return nil, nil, err
			}

			delRowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
			for _, delRowId := range delRowIds {
				b, o := delRowId.Decode()
				if bid.Compare(b) != 0 {
					continue
				}

				leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, o)
				if leftRows != nil && len(leftRows) == 0 {
					done = true
					break
				}
			}

			release()

			if done {
				break
			}
		}

		if done {
			break
		}
	}

	return leftRows, deletedRows, nil
}

func (ls *LocalDataSource) applyWorkspaceRawRowIdDeletes(
	bid objectio.Blockid, offsets []int32,
) (leftRows []int32, deletedRows []int64) {

	leftRows = offsets

	rawRowIdDeletes := ls.table.getTxn().deletedBlocks
	rawRowIdDeletes.RWMutex.RLock()
	defer rawRowIdDeletes.RWMutex.RUnlock()

	for _, o := range rawRowIdDeletes.offsets[bid] {
		leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, uint32(o))
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	return leftRows, deletedRows
}

func (ls *LocalDataSource) applyPStateInMemDeletes(
	bid objectio.Blockid, offsets []int32,
) (leftRows []int32, deletedRows []int64) {

	if ls.rc.SkipPStateDeletes {
		return offsets, deletedRows
	}

	var delIter logtailreplay.RowsIter

	if ls.memPKFilter.SpecFactory == nil {
		delIter = ls.pState.NewRowsIter(ls.snapshotTS, &bid, true)
	} else {
		delIter = ls.pState.NewPrimaryKeyDelIter(
			ls.memPKFilter.TS,
			ls.memPKFilter.SpecFactory(ls.memPKFilter), bid)
	}

	leftRows = offsets

	for delIter.Next() {
		_, o := delIter.Entry().RowID.Decode()
		leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, o)
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	delIter.Close()

	return leftRows, deletedRows
}

func (ls *LocalDataSource) applyPStatePersistedDeltaLocation(
	bid objectio.Blockid, offsets []int32,
) (leftRows []int32, deletedRows []int64, err error) {

	if ls.rc.SkipPStateDeletes {
		return offsets, deletedRows, nil
	}

	var deletes *nulls.Nulls
	deltaLoc, commitTS, ok := ls.pState.GetBockDeltaLoc(bid)
	if ok {
		deletes, err = loadBlockDeletesByDeltaLoc(ls.ctx, ls.fs, bid, deltaLoc[:], ls.snapshotTS, commitTS)
		if err != nil {
			return nil, nil, err
		}

		if offsets != nil {
			for _, offset := range offsets {
				if deletes.Contains(uint64(offset)) {
					continue
				}
				leftRows = append(leftRows, offset)
			}
		} else {
			deletes.Foreach(func(u uint64) bool {
				deletedRows = append(deletedRows, int64(u))
				return true
			})
		}
	} else {
		leftRows = offsets
	}

	return leftRows, deletedRows, nil
}

func (ls *LocalDataSource) batchPrefetch(seqNums []uint16) {
	if ls.rc.batchPrefetchCursor >= len(ls.ranges) ||
		ls.rangesCursor < ls.rc.batchPrefetchCursor {
		return
	}

	batchSize := min(1000, len(ls.ranges)-ls.rc.batchPrefetchCursor)

	begin := ls.rc.batchPrefetchCursor
	end := ls.rc.batchPrefetchCursor + batchSize

	// prefetch blk data
	err := blockio.BlockPrefetchInProgress(
		ls.table.proc.Load().GetService(), seqNums, ls.fs,
		[][]*objectio.BlockInfoInProgress{ls.ranges[begin:end]}, true)
	if err != nil {
		logutil.Errorf("pefetch block data: %s", err.Error())
	}

	// prefetch blk delta location
	for idx := begin; idx < end; idx++ {
		if loc, _, ok := ls.pState.GetBockDeltaLoc(ls.ranges[idx].BlockID); ok {
			if err = blockio.PrefetchTombstone(
				ls.table.proc.Load().GetService(), []uint16{0, 1, 2},
				[]uint16{objectio.Location(loc[:]).ID()}, ls.fs, objectio.Location(loc[:])); err != nil {
				logutil.Errorf("prefetch block delta location: %s", err.Error())
			}
		}
	}

	ls.table.getTxn().blockId_tn_delete_metaLoc_batch.RLock()
	defer ls.table.getTxn().blockId_tn_delete_metaLoc_batch.RUnlock()

	// prefetch cn flushed but not committed deletes
	var ok bool
	var bats []*batch.Batch
	var locs []objectio.Location = make([]objectio.Location, 0)

	pkColIdx := ls.table.tableDef.Pkey.PkeyColId

	for idx := begin; idx < end; idx++ {
		if bats, ok = ls.table.getTxn().blockId_tn_delete_metaLoc_batch.data[ls.ranges[idx].BlockID]; !ok {
			continue
		}

		locs = locs[:0]
		for _, bat := range bats {
			vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
			for i := range vs {
				location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
				if err != nil {
					logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
				}
				locs = append(locs, location)
			}
		}

		if len(locs) == 0 {
			continue
		}

		pref, err := blockio.BuildPrefetchParams(ls.fs, locs[0])
		if err != nil {
			logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
		}

		for _, loc := range locs {
			//rowId + pk
			pref.AddBlockWithType([]uint16{0, uint16(pkColIdx)}, []uint16{loc.ID()}, uint16(objectio.SchemaTombstone))
		}

		if err = blockio.PrefetchWithMerged(ls.table.proc.Load().GetService(), pref); err != nil {
			logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
		}
	}

	ls.rc.batchPrefetchCursor += batchSize
}
