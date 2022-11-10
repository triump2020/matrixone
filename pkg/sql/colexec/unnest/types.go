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

package unnest

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Param struct {
	Attrs    []string
	Cols     []*plan.ColDef
	ExprList []*plan.Expr
	ColName  string
	filters  []string
}

type Argument struct {
	Es *Param
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
}

type ExternalParam struct {
	ColName string
	Path    string
	Outer   bool
}

var (
	deniedFilters = []string{"col", "seq"}
)

const (
	mode      = "both"
	recursive = false
)
