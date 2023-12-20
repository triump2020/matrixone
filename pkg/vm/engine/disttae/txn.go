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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

//func (txn *Transaction) getObjInfos(
//	ctx context.Context,
//	tbl *txnTable,
//) (objs []logtailreplay.ObjectEntry, err error) {
//	ts := types.TimestampToTS(txn.op.SnapshotTS())
//	state, err := tbl.getPartitionState(ctx)
//	if err != nil {
//		return nil, err
//	}
//	iter, err := state.NewObjectsIter(ts)
//	if err != nil {
//		return nil, err
//	}
//	for iter.Next() {
//		entry := iter.Entry()
//		objs = append(objs, entry)
//	}
//	iter.Close()
//	return
//}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly.Load()
}

// WriteBatch used to write data to the transaction buffer
// insert/delete/update all use this api
// insertBatchHasRowId : it denotes the batch has Rowid when the typ is INSERT.
// if typ is not INSERT, it is always false.
// truncate : it denotes the batch with typ DELETE on mo_tables is generated when Truncating
// a table.
func (txn *Transaction) WriteBatch(
	typ int,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	tnStore DNStore,
	primaryIdx int, // pass -1 to indicate no primary key or disable primary key checking
	insertBatchHasRowId bool,
	truncate bool) error {
	txn.readOnly.Store(false)
	bat.Cnt = 1
	txn.Lock()
	defer txn.Unlock()
	if typ == INSERT {
		if !insertBatchHasRowId {
			txn.genBlock()
			len := bat.RowCount()
			vec := txn.proc.GetVector(types.T_Rowid.ToType())
			for i := 0; i < len; i++ {
				if err := vector.AppendFixed(vec, txn.genRowId(), false,
					txn.proc.Mp()); err != nil {
					return err
				}
			}
			bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
			bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
		}
		if tableId != catalog.MO_DATABASE_ID &&
			tableId != catalog.MO_TABLES_ID && tableId != catalog.MO_COLUMNS_ID {
			txn.workspaceSize += uint64(bat.Size())
		}
	}
	e := Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		tnStore:      tnStore,
		truncate:     truncate,
	}
	txn.writes = append(txn.writes, e)
	return nil
}

func (txn *Transaction) dumpBatch(offset int) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.dumpBatchLocked(offset)
}

func checkPKDupGeneric[T comparable](
	t *types.Type,
	attr string,
	vals []T,
	start, count int) error {
	mp := make(map[T]bool)
	for _, v := range vals[start : start+count] {
		if _, ok := mp[v]; ok {
			entry := common.TypeStringValue(*t, v, false)
			return moerr.NewDuplicateEntryNoCtx(entry, attr)
		}
		mp[v] = true
	}
	return nil
}

func checkPKDup(
	pk *vector.Vector,
	attr string,
	start, count int) error {
	mp := make(map[any]bool)
	colType := pk.GetType()
	switch colType.Oid {
	case types.T_bool:
		vs := vector.MustFixedCol[bool](pk)
		return checkPKDupGeneric[bool](colType, attr, vs, start, count)
	case types.T_int8:
		vs := vector.MustFixedCol[int8](pk)
		return checkPKDupGeneric[int8](colType, attr, vs, start, count)
	case types.T_int16:
		vs := vector.MustFixedCol[int16](pk)
		return checkPKDupGeneric[int16](colType, attr, vs, start, count)
	case types.T_int32:
		vs := vector.MustFixedCol[int32](pk)
		return checkPKDupGeneric[int32](colType, attr, vs, start, count)
	case types.T_int64:
		vs := vector.MustFixedCol[int64](pk)
		return checkPKDupGeneric[int64](colType, attr, vs, start, count)
	case types.T_uint8:
		vs := vector.MustFixedCol[uint8](pk)
		return checkPKDupGeneric[uint8](colType, attr, vs, start, count)
	case types.T_uint16:
		vs := vector.MustFixedCol[uint16](pk)
		return checkPKDupGeneric[uint16](colType, attr, vs, start, count)
	case types.T_uint32:
		vs := vector.MustFixedCol[uint32](pk)
		return checkPKDupGeneric[uint32](colType, attr, vs, start, count)
	case types.T_uint64:
		vs := vector.MustFixedCol[uint64](pk)
		return checkPKDupGeneric[uint64](colType, attr, vs, start, count)
	case types.T_decimal64:
		vs := vector.MustFixedCol[types.Decimal64](pk)
		return checkPKDupGeneric[types.Decimal64](colType, attr, vs, start, count)
	case types.T_decimal128:
		vs := vector.MustFixedCol[types.Decimal128](pk)
		return checkPKDupGeneric[types.Decimal128](colType, attr, vs, start, count)
	case types.T_uuid:
		vs := vector.MustFixedCol[types.Uuid](pk)
		return checkPKDupGeneric[types.Uuid](colType, attr, vs, start, count)
	case types.T_float32:
		vs := vector.MustFixedCol[float32](pk)
		return checkPKDupGeneric[float32](colType, attr, vs, start, count)
	case types.T_float64:
		vs := vector.MustFixedCol[float64](pk)
		return checkPKDupGeneric[float64](colType, attr, vs, start, count)
	case types.T_date:
		vs := vector.MustFixedCol[types.Date](pk)
		return checkPKDupGeneric[types.Date](colType, attr, vs, start, count)
	case types.T_timestamp:
		vs := vector.MustFixedCol[types.Timestamp](pk)
		return checkPKDupGeneric[types.Timestamp](colType, attr, vs, start, count)
	case types.T_time:
		vs := vector.MustFixedCol[types.Time](pk)
		return checkPKDupGeneric[types.Time](colType, attr, vs, start, count)
	case types.T_datetime:
		vs := vector.MustFixedCol[types.Datetime](pk)
		return checkPKDupGeneric[types.Datetime](colType, attr, vs, start, count)
	case types.T_enum:
		vs := vector.MustFixedCol[types.Enum](pk)
		return checkPKDupGeneric[types.Enum](colType, attr, vs, start, count)
	case types.T_TS:
		vs := vector.MustFixedCol[types.TS](pk)
		return checkPKDupGeneric[types.TS](colType, attr, vs, start, count)
	case types.T_Rowid:
		vs := vector.MustFixedCol[types.Rowid](pk)
		return checkPKDupGeneric[types.Rowid](colType, attr, vs, start, count)
	case types.T_Blockid:
		vs := vector.MustFixedCol[types.Blockid](pk)
		return checkPKDupGeneric[types.Blockid](colType, attr, vs, start, count)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		for i := start; i < start+count; i++ {
			v := pk.GetStringAt(i)
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, []byte(v), false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			mp[v] = true
		}
	case types.T_array_float32:
		//vec := col.GetDownstreamVector()
		for i := start; i < start+count; i++ {
			v := types.ArrayToString[float32](vector.GetArrayAt[float32](pk, i))
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, pk.GetBytesAt(i), false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			mp[v] = true
		}
	case types.T_array_float64:
		//vec := col.GetDownstreamVector()
		for i := start; i < start+count; i++ {
			v := types.ArrayToString[float64](vector.GetArrayAt[float64](pk, i))
			if _, ok := mp[v]; ok {
				entry := common.TypeStringValue(*colType, pk.GetBytesAt(i), false)
				return moerr.NewDuplicateEntryNoCtx(entry, attr)
			}
			mp[v] = true
		}
	default:
		panic(moerr.NewInternalErrorNoCtx("%s not supported", pk.GetType().String()))
	}
	return nil
}

// checkDup check whether the txn.writes has duplicate pk entry
func (txn *Transaction) checkDup() error {
	tablesDef := make(map[[2]string]*plan.TableDef)
	pkIndex := make(map[[2]string]int)

	for _, e := range txn.writes {
		if e.bat == nil || e.bat.RowCount() == 0 {
			continue
		}
		if e.fileName != "" {
			continue
		}
		if e.typ == INSERT {
			key := [2]string{e.databaseName, e.tableName}
			if _, ok := tablesDef[key]; !ok {
				tbl, err := txn.getTable(key)
				if err != nil {
					return err
				}
				tablesDef[key] = tbl.GetTableDef(txn.proc.Ctx)
			}
			tableDef := tablesDef[key]
			if _, ok := pkIndex[key]; !ok {
				for idx, colDef := range tableDef.Cols {
					//FIXME::tableDef.PKey is nil if table is mo_tables, mo_columns, mo_database?
					if tableDef.Pkey == nil {
						if colDef.Primary {
							pkIndex[key] = idx
							break
						}
					} else {
						if colDef.Name == tableDef.Pkey.PkeyColName {
							if colDef.Name == catalog.FakePrimaryKeyColName {
								pkIndex[key] = -1
							} else {
								pkIndex[key] = idx
							}
							break
						}
					}
				}
			}
			bat := e.bat
			if index := pkIndex[key]; index != -1 {
				if *bat.Vecs[0].GetType() == types.T_Rowid.ToType() {
					newBat := batch.NewWithSize(len(bat.Vecs) - 1)
					newBat.SetAttributes(bat.Attrs[1:])
					newBat.Vecs = bat.Vecs[1:]
					newBat.SetRowCount(bat.Vecs[0].Length())
					bat = newBat
				}
				if err := checkPKDup(bat.Vecs[index], bat.Attrs[index], 0, bat.RowCount()); err != nil {
					return err
				}
			}
			continue
		}
		//if entry.tyep is DELETE, then e.bat.Vecs[0] is rowid,e.bat.Vecs[1] is PK
		pkVec := e.bat.Vecs[1]
		if err := checkPKDup(pkVec, e.bat.Attrs[1], 0, e.bat.RowCount()); err != nil {
			return err
		}
	}
	return nil
}

// dumpBatch if txn.workspaceSize is larger than threshold, cn will write workspace to s3
func (txn *Transaction) dumpBatchLocked(offset int) error {
	var size uint64
	if txn.workspaceSize < WorkspaceThreshold {
		return nil
	}

	txn.hasS3Op.Store(true)
	mp := make(map[[2]string][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].tableId == catalog.MO_DATABASE_ID ||
			txn.writes[i].tableId == catalog.MO_TABLES_ID ||
			txn.writes[i].tableId == catalog.MO_COLUMNS_ID {
			continue
		}
		if txn.writes[i].bat == nil || txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			key := [2]string{txn.writes[i].databaseName, txn.writes[i].tableName}
			bat := txn.writes[i].bat
			size += uint64(bat.Size())
			// skip rowid
			//it's dangerous.
			//bat.Attrs = bat.Attrs[1:]
			//bat.Vecs = bat.Vecs[1:]
			//mp[key] = append(mp[key], bat)

			newBat := batch.NewWithSize(len(bat.Vecs) - 1)
			newBat.SetAttributes(bat.Attrs[1:])
			newBat.Vecs = bat.Vecs[1:]
			newBat.SetRowCount(bat.Vecs[0].Length())
			mp[key] = append(mp[key], newBat)
			txn.toFreeBatches[key] = append(txn.toFreeBatches[key], bat)

			// DON'T MODIFY THE IDX OF AN ENTRY IN LOG
			// THIS IS VERY IMPORTANT FOR CN BLOCK COMPACTION
			// maybe this will cause that the log increments unlimitedly
			// txn.writes = append(txn.writes[:i], txn.writes[i+1:]...)
			// i--
			txn.writes[i].bat = nil
		}
	}

	for key := range mp {
		// scenario 2 for cn write s3, more info in the comment of S3Writer
		tbl, err := txn.getTable(key)
		if err != nil {
			return err
		}

		tableDef := tbl.GetTableDef(txn.proc.Ctx)

		s3Writer, err := colexec.AllocS3Writer(txn.proc, tableDef)
		if err != nil {
			return err
		}
		defer s3Writer.Free(txn.proc)

		s3Writer.InitBuffers(txn.proc, mp[key][0])
		for i := 0; i < len(mp[key]); i++ {
			s3Writer.Put(mp[key][i], txn.proc)
		}
		err = s3Writer.SortAndFlush(txn.proc)

		if err != nil {
			return err
		}
		blockInfo := s3Writer.GetBlockInfoBat()

		lenVecs := len(blockInfo.Attrs)
		// only remain the metaLoc col and object stats
		blockInfo.Vecs = blockInfo.Vecs[lenVecs-2:]
		blockInfo.Attrs = blockInfo.Attrs[lenVecs-2:]
		blockInfo.SetRowCount(blockInfo.Vecs[0].Length())

		table := tbl.(*txnTable)
		fileName := catalog.DecodeBlockInfo(
			blockInfo.Vecs[0].GetBytesAt(0)).
			MetaLocation().Name().String()
		err = table.db.txn.WriteFileLocked(
			INSERT,
			table.db.databaseId,
			table.tableId,
			table.db.databaseName,
			table.tableName,
			fileName,
			blockInfo,
			table.db.txn.tnStores[0],
		)
		if err != nil {
			return err
		}
	}
	if offset == 0 {
		txn.workspaceSize = 0
		writes := txn.writes[:0]
		for i, write := range txn.writes {
			if write.bat != nil {
				writes = append(writes, txn.writes[i])
			}
		}
		txn.writes = writes
		txn.statements[txn.statementID-1] = len(txn.writes)
	} else {
		txn.workspaceSize -= size
	}
	return nil
}

func (txn *Transaction) getTable(key [2]string) (engine.Relation, error) {
	databaseName := key[0]
	tableName := key[1]

	database, err := txn.engine.Database(txn.proc.Ctx, databaseName, txn.proc.TxnOperator)
	if err != nil {
		return nil, err
	}
	tbl, err := database.Relation(txn.proc.Ctx, tableName, nil)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

// vec contains block infos.
func (txn *Transaction) insertPosForCNBlock(
	vec *vector.Vector,
	b *batch.Batch,
	dbName string,
	tbName string) error {
	blks := vector.MustBytesCol(vec)
	for i, blk := range blks {
		blkInfo := *catalog.DecodeBlockInfo(blk)
		txn.cnBlkId_Pos[blkInfo.BlockID] = Pos{
			bat:     b,
			dbName:  dbName,
			tbName:  tbName,
			offset:  int64(i),
			blkInfo: blkInfo}
	}
	return nil
}

func (txn *Transaction) WriteFileLocked(
	typ int,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	tnStore DNStore) error {
	txn.hasS3Op.Store(true)
	newBat := bat
	if typ == INSERT {
		newBat = batch.NewWithSize(len(bat.Vecs))
		newBat.SetAttributes([]string{catalog.BlockMeta_MetaLoc, catalog.ObjectMeta_ObjectStats})

		for idx := 0; idx < newBat.VectorCount(); idx++ {
			newBat.SetVector(int32(idx), vector.NewVec(*bat.Vecs[idx].GetType()))
		}

		blkInfosVec := bat.Vecs[0]
		for idx := 0; idx < blkInfosVec.Length(); idx++ {
			blkInfo := *catalog.DecodeBlockInfo(blkInfosVec.GetBytesAt(idx))
			vector.AppendBytes(newBat.Vecs[0], []byte(blkInfo.MetaLocation().String()),
				false, txn.proc.Mp())
		}

		// append obj stats, may multiple
		statsListVec := bat.Vecs[1]
		for idx := 0; idx < statsListVec.Length(); idx++ {
			vector.AppendBytes(newBat.Vecs[1], statsListVec.GetBytesAt(idx), false, txn.proc.Mp())
		}

		newBat.SetRowCount(bat.Vecs[0].Length())
	}
	txn.readOnly.Store(false)
	entry := Entry{
		typ:          typ,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          newBat,
		tnStore:      tnStore,
	}
	txn.writes = append(txn.writes, entry)
	//the first part of the file name is the segment id which generated by cn through writing S3.
	if sid, err := types.ParseUuid(strings.Split(fileName, "_")[0]); err != nil {
		panic("fileName parse Uuid error")
	} else {
		// get uuid string
		if typ == INSERT {
			colexec.Srv.PutCnSegment(&sid, colexec.CnBlockIdType)
			txn.insertPosForCNBlock(
				bat.GetVector(0),
				entry.bat,
				entry.databaseName,
				entry.tableName)
		}
	}
	return nil
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(
	typ int,
	databaseId,
	tableId uint64,
	databaseName,
	tableName string,
	fileName string,
	bat *batch.Batch,
	tnStore DNStore) error {
	txn.Lock()
	defer txn.Unlock()
	return txn.WriteFileLocked(
		typ, databaseId, tableId,
		databaseName, tableName, fileName, bat, tnStore)
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {
	mp := make(map[types.Rowid]uint8)
	deleteBlkId := make(map[types.Blockid]bool)
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(0))
	min1 := uint32(math.MaxUint32)
	max1 := uint32(0)
	cnRowIdOffsets := make([]int64, 0, len(rowids))
	for i, rowid := range rowids {
		// process cn block deletes
		uid := rowid.BorrowSegmentID()
		blkid := rowid.CloneBlockID()
		deleteBlkId[blkid] = true
		mp[rowid] = 0
		rowOffset := rowid.GetRowOffset()
		if colexec.Srv != nil && colexec.Srv.GetCnSegmentType(uid) == colexec.CnBlockIdType {
			txn.deletedBlocks.addDeletedBlocks(&blkid, []int64{int64(rowOffset)})
			cnRowIdOffsets = append(cnRowIdOffsets, int64(i))
			continue
		}
		if rowOffset < (min1) {
			min1 = rowOffset
		}

		if rowOffset > max1 {
			max1 = rowOffset
		}
		// update workspace
	}
	// cn rowId antiShrink
	bat.AntiShrink(cnRowIdOffsets)
	if bat.RowCount() == 0 {
		return bat
	}
	sels := txn.proc.Mp().GetSels()
	txn.deleteTableWrites(databaseId, tableId, sels, deleteBlkId, min1, max1, mp)
	sels = sels[:0]
	for k, rowid := range rowids {
		if mp[rowid] == 0 {
			sels = append(sels, int64(k))
		}
	}
	bat.Shrink(sels)
	txn.proc.Mp().PutSels(sels)
	return bat
}

func (txn *Transaction) deleteTableWrites(
	databaseId uint64,
	tableId uint64,
	sels []int64,
	deleteBlkId map[types.Blockid]bool,
	min, max uint32,
	mp map[types.Rowid]uint8,
) {
	txn.Lock()
	defer txn.Unlock()

	// txn worksapce will have four batch type:
	// 1.RawBatch 2.DN Block RowId(mixed rowid from different block)
	// 3.CN block Meta batch(record block meta generated by cn insert write s3)
	// 4.DN delete Block Meta batch(record block meta generated by cn delete write s3)
	for _, e := range txn.writes {
		// nil batch will generated by comapction or dumpBatch
		if e.bat == nil {
			continue
		}
		// for 3 and 4 above.
		if e.bat.Attrs[0] == catalog.BlockMeta_MetaLoc ||
			e.bat.Attrs[0] == catalog.BlockMeta_DeltaLoc {
			continue
		}
		sels = sels[:0]
		if e.tableId == tableId && e.databaseId == databaseId {
			vs := vector.MustFixedCol[types.Rowid](e.bat.GetVector(0))
			if len(vs) == 0 {
				continue
			}
			// skip 2 above
			if !vs[0].BorrowSegmentID().Eq(txn.segId) {
				continue
			}
			// current batch is not be deleted
			if !deleteBlkId[vs[0].CloneBlockID()] {
				continue
			}
			min2 := vs[0].GetRowOffset()
			max2 := vs[len(vs)-1].GetRowOffset()
			if min > max2 || max < min2 {
				continue
			}
			for k, v := range vs {
				if _, ok := mp[v]; !ok {
					sels = append(sels, int64(k))
				} else {
					mp[v]++
				}
			}
			if len(sels) != len(vs) {
				txn.batchSelectList[e.bat] = append(txn.batchSelectList[e.bat], sels...)

			}
		}
	}
}

func (txn *Transaction) allocateID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return txn.idGen.AllocateID(ctx)
}

func (txn *Transaction) genBlock() {
	txn.rowId[4]++
	txn.rowId[5] = INIT_ROWID_OFFSET
}

func (txn *Transaction) genRowId() types.Rowid {
	if txn.rowId[5] != INIT_ROWID_OFFSET {
		txn.rowId[5]++
	} else {
		txn.rowId[5] = 0
	}
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
}

func (txn *Transaction) mergeTxnWorkspaceLocked() error {
	if len(txn.batchSelectList) > 0 {
		for _, e := range txn.writes {
			if sels, ok := txn.batchSelectList[e.bat]; ok {
				e.bat.Shrink(sels)
				delete(txn.batchSelectList, e.bat)
			}
		}
	}
	return nil
}

// CN blocks compaction for txn
func (txn *Transaction) mergeCompactionLocked() error {
	compactedBlks := make(map[[2]string]map[catalog.BlockInfo][]int64)
	compactedEntries := make(map[*batch.Batch][]int64)
	defer func() {
		txn.deletedBlocks = nil
	}()
	txn.deletedBlocks.iter(
		func(blkId *types.Blockid, offsets []int64) bool {
			pos := txn.cnBlkId_Pos[*blkId]
			if v, ok := compactedBlks[[2]string{pos.dbName, pos.tbName}]; ok {
				v[pos.blkInfo] = offsets
			} else {
				compactedBlks[[2]string{pos.dbName, pos.tbName}] =
					map[catalog.BlockInfo][]int64{pos.blkInfo: offsets}
			}
			compactedEntries[pos.bat] = append(compactedEntries[pos.bat], pos.offset)
			delete(txn.cnBlkId_Pos, *blkId)
			return true
		})

	for key, blks := range compactedBlks {
		rel, err := txn.getTable(key)
		if err != nil {
			return err
		}
		//TODO::do parallel compaction for table
		tbl := rel.(*txnTable)
		createdBlks, stats, err := tbl.mergeCompaction(blks)
		if err != nil {
			return err
		}
		if len(createdBlks) > 0 {
			bat := batch.NewWithSize(2)
			bat.Attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
			bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
			bat.SetVector(1, vector.NewVec(types.T_binary.ToType()))
			for _, blkInfo := range createdBlks {
				vector.AppendBytes(
					bat.GetVector(0),
					catalog.EncodeBlockInfo(blkInfo),
					false,
					tbl.db.txn.proc.GetMPool())
			}

			// append the object stats to bat
			for idx := 0; idx < len(stats); idx++ {
				if stats[idx].IsZero() {
					continue
				}
				if err = vector.AppendBytes(bat.Vecs[1], stats[idx].Marshal(),
					false, tbl.db.txn.proc.GetMPool()); err != nil {
					return err
				}
			}

			bat.SetRowCount(len(createdBlks))
			defer func() {
				bat.Clean(tbl.db.txn.proc.GetMPool())
			}()

			err := txn.WriteFileLocked(
				INSERT,
				tbl.db.databaseId,
				tbl.tableId,
				tbl.db.databaseName,
				tbl.tableName,
				createdBlks[0].MetaLocation().Name().String(),
				bat,
				tbl.db.txn.tnStores[0],
			)
			if err != nil {
				return err
			}
		}
	}

	//compaction for txn.writes
	for i, entry := range txn.writes {
		if entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}
		if entry.typ != INSERT ||
			entry.bat.Attrs[0] != catalog.BlockMeta_MetaLoc {
			continue
		}
		entry.bat.AntiShrink(compactedEntries[entry.bat])
		if entry.bat.RowCount() == 0 {
			txn.writes[i].bat.Clean(txn.proc.GetMPool())
			txn.writes[i].bat = nil
		}
	}
	return nil
}

func (txn *Transaction) hasDeletesOnUncommitedObject() bool {
	return !txn.deletedBlocks.isEmpty()
}

func (txn *Transaction) hasUncommittedDeletesOnBlock(id *types.Blockid) bool {
	return txn.deletedBlocks.hasDeletes(id)
}

func (txn *Transaction) getUncommitedDataObjectsByTable(
	databaseId uint64, tableId uint64) (statsList []objectio.ObjectStats, err error) {
	txn.Lock()
	defer txn.Unlock()
	var stats objectio.ObjectStats
	for _, entry := range txn.writes {
		if entry.databaseId != databaseId ||
			entry.tableId != tableId {
			continue
		}
		if entry.bat == nil || entry.bat.IsEmpty() {
			continue
		}
		if entry.typ != INSERT || len(entry.bat.Attrs) < 2 ||
			entry.bat.Attrs[1] != catalog.ObjectMeta_ObjectStats {
			continue
		}
		stats.UnMarshal(entry.bat.Vecs[1].GetBytesAt(0))
		statsList = append(statsList, stats)

	}
	return statsList, nil

}

func (txn *Transaction) getTableWrites(databaseId uint64, tableId uint64, writes []Entry) []Entry {
	txn.Lock()
	defer txn.Unlock()
	for _, entry := range txn.writes {
		if entry.databaseId != databaseId {
			continue
		}
		if entry.tableId != tableId {
			continue
		}
		writes = append(writes, entry)
	}
	return writes
}

// getCachedTable returns the cached table in this transaction if it exists, nil otherwise.
// Before it gets the cached table, it checks whether the table is deleted by another
// transaction by go through the delete tables slice, and advance its cachedIndex.
func (txn *Transaction) getCachedTable(
	ctx context.Context, k tableKey, snapshotTS timestamp.Timestamp,
) *txnTable {
	var tbl *txnTable
	if v, ok := txn.tableCache.tableMap.Load(k); ok {
		tbl = v.(*txnTable)

		tblKey := cache.TableKey{
			AccountId:  k.accountId,
			DatabaseId: k.databaseId,
			Name:       k.name,
		}
		val := txn.engine.catalog.GetSchemaVersion(tblKey)
		if val != nil {
			if val.Ts.Greater(tbl.lastTS) && val.Version != tbl.version {
				txn.tableCache.tableMap.Delete(genTableKey(ctx, k.name, k.databaseId))
				return nil
			}
		}

	}
	return tbl
}

func (txn *Transaction) Commit(ctx context.Context) ([]txn.TxnRequest, error) {
	logDebugf(txn.op.Txn(), "Transaction.Commit")
	txn.IncrStatementID(ctx, true)
	defer txn.delTransaction()
	if txn.readOnly.Load() {
		return nil, nil
	}
	if err := txn.mergeTxnWorkspaceLocked(); err != nil {
		return nil, err
	}
	if err := txn.mergeCompactionLocked(); err != nil {
		return nil, err
	}
	if err := txn.dumpBatchLocked(0); err != nil {
		return nil, err
	}
	if err := txn.checkDup(); err != nil {
		return nil, err
	}
	reqs, err := genWriteReqs(ctx, txn.writes, txn.op.Txn().DebugString())
	if err != nil {
		return nil, err
	}
	return reqs, nil
}

func (txn *Transaction) Rollback(ctx context.Context) error {
	logDebugf(txn.op.Txn(), "Transaction.Rollback")
	txn.delTransaction()
	return nil
}

func (txn *Transaction) delTransaction() {
	if txn.removed {
		return
	}
	for i := range txn.writes {
		if txn.writes[i].bat == nil {
			continue
		}
		txn.proc.PutBatch(txn.writes[i].bat)
	}
	txn.tableCache.cachedIndex = -1
	txn.tableCache.tableMap = nil
	txn.createMap = nil
	txn.databaseMap = nil
	txn.deletedTableMap = nil
	txn.blockId_tn_delete_metaLoc_batch = nil
	txn.deletedBlocks = nil
	segmentnames := make([]objectio.Segmentid, 0, len(txn.cnBlkId_Pos)+1)
	segmentnames = append(segmentnames, txn.segId)
	for blkId := range txn.cnBlkId_Pos {
		// blkId:
		// |------|----------|----------|
		//   uuid    filelen   blkoffset
		//    16        2          2
		segmentnames = append(segmentnames, *blkId.Segment())
	}
	colexec.Srv.DeleteTxnSegmentIds(segmentnames)
	txn.cnBlkId_Pos = nil
	txn.hasS3Op.Store(false)
	txn.removed = true
}

func (txn *Transaction) addCreateTable(
	key tableKey,
	value *txnTable) {
	txn.Lock()
	defer txn.Unlock()
	value.createByStatementID = txn.statementID
	txn.createMap.Store(key, value)
}

func (txn *Transaction) rollbackCreateTableLocked() {
	txn.createMap.Range(func(key, value any) bool {
		if value.(*txnTable).createByStatementID == txn.statementID {
			txn.createMap.Delete(key)
		}
		return true
	})
}
