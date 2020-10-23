// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lookupops

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type all struct {
	nbf *types.NomsBinFormat
}

var _ LookupOp = (*all)(nil)

// IsSubsetOf implements LookupOp
func (op *all) IsSubsetOf(otherOp LookupOp) (bool, error) {
	_, isAll := otherOp.(*all)
	if isAll {
		return true, nil
	}
	return false, nil
}

// IsSupersetOf implements LookupOp
func (op *all) IsSupersetOf(LookupOp) (bool, error) {
	return true, nil
}

// Union implements LookupOp
func (op *all) Union(LookupOp) (LookupOp, error) {
	return op, nil
}

// Intersection implements LookupOp
func (op *all) Intersection(otherOp LookupOp) (LookupOp, error) {
	return otherOp, nil
}

// ToReadRange implements LookupOp
func (op *all) ToReadRange() *noms.ReadRange {
	return &noms.ReadRange{
		Start:     types.EmptyTuple(op.nbf),
		Inclusive: true,
		Reverse:   false,
		Check:     alwaysContinueRangeCheck,
	}
}
