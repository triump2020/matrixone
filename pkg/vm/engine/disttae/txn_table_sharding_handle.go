// Copyright 2021-2024 Matrix Origin
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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// HandleShardingReadRows handles sharding read rows
func HandleShardingReadRows(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	rows, err := tbl.Rows(ctx)
	if err != nil {
		return nil, err
	}
	return buffer.EncodeUint64(rows), nil
}

// HandleShardingReadSize handles sharding read size
func HandleShardingReadSize(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	rows, err := tbl.Size(
		ctx,
		param.SizeParam.ColumnName,
	)
	if err != nil {
		return nil, err
	}
	return buffer.EncodeUint64(rows), nil
}

// HandleShardingReadStatus handles sharding read status
func HandleShardingReadStatus(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	info, err := tbl.Stats(
		ctx,
		param.StatsParam.Sync,
	)
	if err != nil {
		return nil, err
	}

	bys, err := info.Marshal()
	if err != nil {
		return nil, err
	}
	return buffer.EncodeBytes(bys), nil
}

// HandleShardingReadApproxObjectsNum handles sharding read ApproxObjectsNum
func HandleShardingReadApproxObjectsNum(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	num := tbl.ApproxObjectsNum(
		ctx,
	)
	return buffer.EncodeInt(num), nil
}

// HandleShardingReadRanges handles sharding read Ranges
func HandleShardingReadRanges(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	var uncommittedRanges []objectio.ObjectStats
	n := len(param.RangesParam.UncommittedObjects) / objectio.ObjectStatsLen
	for i := 0; i < n; i++ {
		var stat objectio.ObjectStats
		stat.UnMarshal(param.RangesParam.UncommittedObjects[i*objectio.ObjectStatsLen : (i+1)*objectio.ObjectStatsLen])
		uncommittedRanges = append(uncommittedRanges, stat)
	}

	ranges, err := tbl.doRanges(
		ctx,
		param.RangesParam.Exprs,
		uncommittedRanges,
	)
	if err != nil {
		return nil, err
	}

	bys, err := ranges.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return buffer.EncodeBytes(bys), nil
}

func HandleShardingReadNext(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	return nil, nil
}

func HandleShardingReadClose(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	return nil, nil
}

func HandleShardingReadCollectTombstones(
	ctx context.Context,
	shard shard.TableShard,
	eng engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		eng,
	)
	if err != nil {
		return nil, err
	}

	tombstones, err := tbl.CollectTombstones(
		ctx,
		0,
		engine.TombstoneCollectPolicy(param.CollectTombstonesParam.CollectPolicy),
	)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = tombstones.MarshalBinaryWithBuffer(&buf)
	if err != nil {
		return nil, err
	}
	return buffer.EncodeBytes(buf.Bytes()), nil
}

// HandleShardingReadGetColumMetadataScanInfo handles sharding read GetColumMetadataScanInfo
func HandleShardingReadGetColumMetadataScanInfo(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	infos, err := tbl.GetColumMetadataScanInfo(
		ctx,
		param.GetColumMetadataScanInfoParam.ColumnName,
	)
	if err != nil {
		return nil, err
	}

	v := plan.MetadataScanInfos{
		Infos: infos,
	}
	bys, err := v.Marshal()
	if err != nil {
		panic(err)
	}
	return buffer.EncodeBytes(bys), nil
}

// HandleShardingReadReader handles sharding read Reader
func HandleShardingReadBuildReader(
	ctx context.Context,
	shard shard.TableShard,
	e engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		e,
	)
	if err != nil {
		return nil, err
	}

	relData, err := UnmarshalRelationData(param.ReaderBuildParam.RelData)
	if err != nil {
		return nil, err
	}
	_, err = tbl.BuildReaders(
		ctx,
		tbl.proc.Load(),
		param.ReaderBuildParam.Expr,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckAll,
	)
	if err != nil {
		return nil, err
	}
	// TODO:
	return nil, nil
}

// HandleShardingReadPrimaryKeysMayBeModified handles sharding read PrimaryKeysMayBeModified
func HandleShardingReadPrimaryKeysMayBeModified(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	var from, to types.TS
	err = from.Unmarshal(param.PrimaryKeysMayBeModifiedParam.From)
	if err != nil {
		return nil, err
	}

	err = to.Unmarshal(param.PrimaryKeysMayBeModifiedParam.To)
	if err != nil {
		return nil, err
	}

	keyVector := vector.NewVecFromReuse()
	err = keyVector.UnmarshalBinary(param.PrimaryKeysMayBeModifiedParam.KeyVector)
	if err != nil {
		return nil, err
	}

	modify, err := tbl.PrimaryKeysMayBeModified(
		ctx,
		from,
		to,
		keyVector,
	)
	if err != nil {
		return nil, err
	}
	var r uint16
	if modify {
		r = 1
	}
	return buffer.EncodeUint16(r), nil
}

// HandleShardingReadMergeObjects handles sharding read MergeObjects
func HandleShardingReadMergeObjects(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	objstats := make([]objectio.ObjectStats, len(param.MergeObjectsParam.Objstats))
	for i, o := range param.MergeObjectsParam.Objstats {
		objstats[i].UnMarshal(o)
	}

	entry, err := tbl.MergeObjects(
		ctx,
		objstats,
		param.MergeObjectsParam.TargetObjSize,
	)
	if err != nil {
		return nil, err
	}

	bys, err := entry.Marshal()
	if err != nil {
		return nil, err
	}
	return buffer.EncodeBytes(bys), nil
}

func HandleShardingReadVisibleObjectStats(
	ctx context.Context,
	shard shard.TableShard,
	engine engine.Engine,
	param shard.ReadParam,
	ts timestamp.Timestamp,
	buffer *morpc.Buffer,
) ([]byte, error) {
	tbl, err := getTxnTable(
		ctx,
		param,
		engine,
	)
	if err != nil {
		return nil, err
	}

	stats, err := tbl.GetNonAppendableObjectStats(ctx)
	if err != nil {
		return nil, err
	}

	b := new(bytes.Buffer)
	size := len(stats)
	marshalSize := size * (objectio.ObjectStatsLen)
	b.Grow(marshalSize)
	for _, stat := range stats {
		b.Write(stat.Marshal())
	}
	return buffer.EncodeBytes(b.Bytes()), nil
}

func getTxnTable(
	ctx context.Context,
	param shard.ReadParam,
	engine engine.Engine,
) (*txnTable, error) {
	// TODO: reduce mem allocate
	proc, err := process.GetCodecService(engine.GetService()).Decode(
		ctx,
		param.Process,
	)
	if err != nil {
		return nil, err
	}

	db := &txnDatabase{
		op:           proc.GetTxnOperator(),
		databaseName: param.TxnTable.DatabaseName,
		databaseId:   param.TxnTable.DatabaseID,
	}

	item, err := db.getTableItem(
		ctx,
		uint32(param.TxnTable.AccountID),
		param.TxnTable.TableName,
		engine.(*Engine),
	)
	if err != nil {
		return nil, err
	}

	return newTxnTableWithItem(
		db,
		item,
		proc,
	), nil
}
