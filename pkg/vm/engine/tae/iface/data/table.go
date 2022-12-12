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

package data

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	ErrAppendableBlockNotFound   = moerr.NewAppendableBlockNotFoundNoCtx()
	ErrAppendableSegmentNotFound = moerr.NewAppendableSegmentNotFoundNoCtx()
	ErrNonAppendableSegmentNotFound = moerr.NewNonAppendableSegmentNotFound()
)

type TableHandle interface {
	GetAppender() (BlockAppender, error)
	SetAppender(*common.ID) BlockAppender
}

type Table interface {
	GetHandle() TableHandle
	ApplyHandle(TableHandle)
	GetLastNonAppendableSeg() (*common.ID, error)
	CloseLastNonAppendableSeg()
	SetLastNonAppendableSeg(id *common.ID)
}
