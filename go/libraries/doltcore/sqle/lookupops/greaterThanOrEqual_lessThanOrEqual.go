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

type greaterThanOrEqual_lessThanOrEqual struct { // underscore is for clarity
	nbf    *types.NomsBinFormat
	gteKey types.Tuple
	lteKey types.Tuple
}

var _ LookupOp = (*greaterThanOrEqual_lessThanOrEqual)(nil)

//TODO: remove all the ending returns

func (op *greaterThanOrEqual_lessThanOrEqual) IsSubsetOf(otherOp LookupOp) (bool, error) {
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

func (op *greaterThanOrEqual_lessThanOrEqual) IsSupersetOf(otherOp LookupOp) (bool, error) {
	return otherOp.IsSubsetOf(op)
}

func (op *greaterThanOrEqual_lessThanOrEqual) Union(otherOp LookupOp) (LookupOp, error) {
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

func (op *greaterThanOrEqual_lessThanOrEqual) Intersection(otherOp LookupOp) (LookupOp, error) {
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
func (op *greaterThanOrEqual_lessThanOrEqual) ToReadRange() *noms.ReadRange {
	// In the case of possible partial keys, we need to match at the end for matched values, so we append a tag that is
	// beyond the allowed maximum. This will be ignored if it's a full key and not a partial key.
	lteKey, err := op.lteKey.Append(types.Uint(uint64(0xffffffffffffffff)))
	if err != nil {
		panic(err) // should never happen
	}
	return &noms.ReadRange{
		Start:     op.gteKey,
		Inclusive: true,
		Reverse:   false,
		Check: func(tuple types.Tuple) (bool, error) {
			return tuple.Less(op.nbf, lteKey)
		},
	}
}
