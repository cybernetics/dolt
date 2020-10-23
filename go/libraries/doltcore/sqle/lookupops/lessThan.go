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

type lessThan struct {
	nbf *types.NomsBinFormat
	key types.Tuple
}

var _ LookupOp = (*lessThan)(nil)

//TODO: remove all the ending returns

func (op *lessThan) IsSubsetOf(otherOp LookupOp) (bool, error) {
	switch other := otherOp.(type) {
	case *all:
	case *none:
	case *equals:
	case *greaterThan:
	case *greaterThanOrEqual:
	case *lessThan:
	case *lessThanOrEqual:
	case *greaterThan_lessThan:
	case *greaterThan_lessThanOrEqual:
	case *greaterThanOrEqual_lessThan:
	case *greaterThanOrEqual_lessThanOrEqual:
	default:
		return false, ErrLookupOpUnhandledType.New(other)
	}
	return false, nil
}

func (op *lessThan) IsSupersetOf(otherOp LookupOp) (bool, error) {
	return otherOp.IsSubsetOf(op)
}

func (op *lessThan) Union(otherOp LookupOp) (LookupOp, error) {
	switch other := otherOp.(type) {
	case *all:
	case *none:
	case *equals:
	case *greaterThan:
	case *greaterThanOrEqual:
	case *lessThan:
	case *lessThanOrEqual:
	case *greaterThan_lessThan:
	case *greaterThan_lessThanOrEqual:
	case *greaterThanOrEqual_lessThan:
	case *greaterThanOrEqual_lessThanOrEqual:
	default:
		return nil, ErrLookupOpUnhandledType.New(other)
	}
	return nil, nil
}

func (op *lessThan) Intersection(otherOp LookupOp) (LookupOp, error) {
	switch other := otherOp.(type) {
	case *all:
	case *none:
	case *equals:
	case *greaterThan:
	case *greaterThanOrEqual:
	case *lessThan:
	case *lessThanOrEqual:
	case *greaterThan_lessThan:
	case *greaterThan_lessThanOrEqual:
	case *greaterThanOrEqual_lessThan:
	case *greaterThanOrEqual_lessThanOrEqual:
	default:
		return nil, ErrLookupOpUnhandledType.New(other)
	}
	return nil, nil
}

// ToReadRange implements LookupOp
func (op *lessThan) ToReadRange() *noms.ReadRange {
	return &noms.ReadRange{
		Start:     op.key,
		Inclusive: false,
		Reverse:   true,
		Check:     alwaysContinueRangeCheck,
	}
}
