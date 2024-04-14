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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// init is used to insert some data that will not be synchronized by logtail.
func (e *Engine) init(ctx context.Context) error {
	e.Lock()
	defer e.Unlock()
	defer func() {
		dbseq := e.catalog.GetTableById(catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID)
		tbseq := e.catalog.GetTableById(catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID)
		colsql := e.catalog.GetTableById(catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID)
		logutil.Infof("xxxx init dbseq: %d, tbseq: %d, colseq: %d",
			dbseq.PrimarySeqnum, tbseq.PrimarySeqnum, colsql.PrimarySeqnum)
		logutil.Infof("xxxx init dbcons: %s, tbcons: %s, colcons: %s",
			string(dbseq.Constraint), string(tbseq.Constraint), string(colsql.Constraint))
	}()
	m := e.mp

	e.catalog = cache.NewCatalog()
	e.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	var packer *types.Packer
	put := e.packerPool.Get(&packer)
	defer put.Put()

	{
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] = logtailreplay.NewPartition()
	}

	{
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] = logtailreplay.NewPartition()
	}

	{
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] = logtailreplay.NewPartition()
	}

	{ // mo_catalog
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}]
		bat, err := genCreateDatabaseTuple("", 0, 0, 0, catalog.MO_CATALOG, catalog.MO_CATALOG_ID, "", m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF, packer)
		done()
		e.catalog.InsertDatabase(bat)
		bat.Clean(m)
	}

	{ // mo_database
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := genColumns(0, catalog.MO_DATABASE, catalog.MO_CATALOG, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MoDatabaseTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0,
			catalog.MO_DATABASE, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetRowCount(len(cols))
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	{ // mo_tables
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := genColumns(0, catalog.MO_TABLES, catalog.MO_CATALOG, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MoTablesTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_TABLES, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetRowCount(len(cols))
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	{ // mo_columns
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := genColumns(0, catalog.MO_COLUMNS, catalog.MO_CATALOG, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MoColumnsTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_COLUMNS, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetRowCount(len(cols))
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	return nil
}

// TODO:: primarySeqnum is 0 for mo_tables, mo_columns, mo_columns, need to handle this case.
func (e *Engine) getOrCreateSnapPart(
	ctx context.Context,
	databaseId uint64,
	dbName string,
	tableId uint64,
	tblName string,
	primaySeqnum int,
	ts types.TS) (*logtailreplay.Partition, error) {

	//First, check whether the latest partition is available.
	e.Lock()
	partition, ok := e.partitions[[2]uint64{databaseId, tableId}]
	e.Unlock()
	if ok && partition.CanServe(ts) {
		return partition, nil
	}

	//Then, check whether the snapshot partitions is available.
	e.mu.Lock()
	snaps, ok := e.mu.snapParts[[2]uint64{databaseId, tableId}]
	if !ok {
		e.mu.snapParts[[2]uint64{databaseId, tableId}] = &struct {
			sync.Mutex
			snaps []*logtailreplay.Partition
		}{}
		snaps = e.mu.snapParts[[2]uint64{databaseId, tableId}]
	}
	e.mu.Unlock()

	snaps.Lock()
	defer snaps.Unlock()
	for _, snap := range snaps.snaps {
		if snap.CanServe(ts) {
			return snap, nil
		}
	}
	//new snapshot partition and apply checkpoints into it.
	snap := logtailreplay.NewPartition()
	ckps, err := checkpoint.ListSnapshotCheckpoint(ctx, e.fs, ts, tableId)
	if err != nil {
		return nil, err
	}
	//TODO:: handle mo_catalog, mo_database, mo_tables, mo_columns,
	//       put them into snapshot partition.
	snap.ConsumeSnapCkps(ctx, ckps, func(
		checkpoint *checkpoint.CheckpointEntry,
		state *logtailreplay.PartitionState) error {
		loc := checkpoint.GetLocation().String()
		entries, closeCBs, err := logtail.LoadCheckpointEntries(
			ctx,
			loc,
			tableId,
			tblName,
			databaseId,
			dbName,
			e.mp,
			e.fs)
		if err != nil {
			return err
		}
		defer func() {
			for _, cb := range closeCBs {
				cb()
			}
		}()
		for _, entry := range entries {
			if err = consumeEntry(ctx, primaySeqnum, e, nil, state, entry); err != nil {
				return err
			}
		}
		return nil
	})
	snaps.snaps = append(snaps.snaps, snap)

	return snap, nil
}

func (e *Engine) getOrCreateLatestPart(databaseId, tableId uint64) *logtailreplay.Partition {
	e.Lock()
	defer e.Unlock()
	partition, ok := e.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		partition = logtailreplay.NewPartition()
		e.partitions[[2]uint64{databaseId, tableId}] = partition
	}
	return partition
}

func (e *Engine) lazyLoad(ctx context.Context, tbl *txnTable) (*logtailreplay.Partition, error) {
	part := e.getOrCreateLatestPart(tbl.db.databaseId, tbl.tableId)

	if err := part.ConsumeCheckpoints(
		ctx,
		func(checkpoint string, state *logtailreplay.PartitionState) error {
			entries, closeCBs, err := logtail.LoadCheckpointEntries(
				ctx,
				checkpoint,
				tbl.tableId,
				tbl.tableName,
				tbl.db.databaseId,
				tbl.db.databaseName,
				tbl.getTxn().engine.mp,
				tbl.getTxn().engine.fs)
			if err != nil {
				return err
			}
			defer func() {
				for _, cb := range closeCBs {
					cb()
				}
			}()
			for _, entry := range entries {
				if err = consumeEntry(ctx, tbl.primarySeqnum, e, e.catalog, state, entry); err != nil {
					return err
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	return part, nil
}

func (e *Engine) UpdateOfPush(ctx context.Context, databaseId, tableId uint64, ts timestamp.Timestamp) error {
	return e.pClient.TryToSubscribeTable(ctx, databaseId, tableId)
}
