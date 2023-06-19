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

package output

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("sql output")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(_ int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	if bat := proc.Reg.InputBatch; bat != nil && bat.Length() > 0 {
		// WTF
		for i := range bat.Zs {
			bat.Zs[i] = 1
		}

		if bat.VectorCount() == 2 {
			v := bat.GetVector(1)
			if v.GetType().Oid == types.T_int32 {
				vs := vector.MustFixedCol[int32](v)
				if len(vs) == 1 {
					fmt.Printf("txn %s output %d\n", hex.EncodeToString(proc.TxnOperator.Txn().ID), vs[0])
				}
			}
		}

		if err := ap.Func(ap.Data, bat); err != nil {
			proc.PutBatch(bat)
			return false, err
		}
		proc.PutBatch(bat)
	}
	return false, nil
}
