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

type equals struct {
	nbf *types.NomsBinFormat
	key types.Tuple
}

var _ LookupOp = (*equals)(nil)

// IsSubsetOf implements LookupOp
func (op *equals) IsSubsetOf(otherOp LookupOp) (bool, error) {
	switch other := otherOp.(type) {
	case *all:
		return true, nil
	case *none:
		return false, nil
	case *equals:
		// ==[2,1] is subset of ==[2]
		return op.key.StartsWith(other.key), nil
	case *greaterThan:
		// ==[2] ==[2,2] are not subsets of >[2,2]
		if other.key.StartsWith(op.key) {
			return false, nil
		}
		// ==[3] ==[2,3] are subsets of >[2,2]
		return other.key.Less(other.key.Format(), op.key)
	case *greaterThanOrEqual:
		// ==[2,2] is subset of >=[2,2]
		if op.key.Equals(other.key) {
			return true, nil
		}
		// ==[2] is not subset of >=[2,2]
		if other.key.StartsWith(op.key) {
			return false, nil
		}
		// ==[3] ==[2,3] are subsets of >=[2,2]
		return other.key.Less(other.key.Format(), op.key)
	case *lessThan:
		// ==[2] is not subset of <[2,2]
		if other.key.StartsWith(op.key) {
			return false, nil
		}
		// ==[1] ==[1,1] are subsets of <[2,2]
		return op.key.Less(op.key.Format(), other.key)
	case *lessThanOrEqual:
		// ==[2,2] is subset of <=[2,2]
		if op.key.Equals(other.key) {
			return true, nil
		}
		// ==[2] is not subset of <=[2,2]
		if other.key.StartsWith(op.key) {
			return false, nil
		}
		// ==[1,1] ==[2,1] are subsets of <=[2,2]
		return op.key.Less(op.key.Format(), other.key)
	case *greaterThan_lessThan: //TODO: continue this
	case *greaterThan_lessThanOrEqual:
	case *greaterThanOrEqual_lessThan:
	case *greaterThanOrEqual_lessThanOrEqual:
	default:
		return false, ErrLookupOpUnhandledType.New(other)
	}
	return false, nil //TODO: remove this
}

// IsSupersetOf implements LookupOp
func (op *equals) IsSupersetOf(otherOp LookupOp) (bool, error) {
	return otherOp.IsSubsetOf(op)
}

// Union implements LookupOp
func (op *equals) Union(otherOp LookupOp) (LookupOp, error) {
	switch other := otherOp.(type) {
	case *all:
		return other, nil
	case *none:
		return op, nil
	case *equals:
		// if one is a subset then the union is just the superset
		ok, err := op.IsSubsetOf(other)
		if err != nil {
			return nil, err
		}
		if ok {
			return other, nil
		}
		ok, err = op.IsSupersetOf(other)
		if err != nil {
			return nil, err
		}
		if ok {
			return op, nil
		}
		// if neither is a subset of the other then they don't overlap
		return nil, nil
	case *greaterThan: //TODO: continue this
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
	return nil, nil //TODO: remove this
}

// Intersection implements LookupOp
func (op *equals) Intersection(otherOp LookupOp) (LookupOp, error) {
	switch other := otherOp.(type) {
	case *all:
		return op, nil
	case *none:
		return otherOp, nil
	case *equals:
		// if one is a subset then the intersection is just the subset
		ok, err := op.IsSubsetOf(other)
		if err != nil {
			return nil, err
		}
		if ok {
			return op, nil
		}
		ok, err = op.IsSupersetOf(other)
		if err != nil {
			return nil, err
		}
		if ok {
			return other, nil
		}
		// if neither is a subset of the other then they don't overlap
		return NewLookupOp_None(op.nbf), nil
	case *greaterThan: //TODO: continue this
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
	return nil, nil //TODO: remove this
}

// ToReadRange implements LookupOp
func (op *equals) ToReadRange() *noms.ReadRange {
	return &noms.ReadRange{
		Start:     op.key,
		Inclusive: true,
		Reverse:   false,
		Check: func(tuple types.Tuple) (bool, error) {
			return tuple.StartsWith(op.key), nil
		},
	}
}
