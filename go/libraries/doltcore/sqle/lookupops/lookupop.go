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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// LookupOp is a lookup operation that may be converted into a ReadRange for iterating over an index.
type LookupOp interface {
	// IsSubsetOf returns whether the calling LookupOp will return a result set that is equivalent to or contained
	// within the given LookupOp's result set.
	IsSubsetOf(LookupOp) (bool, error)
	// IsSupersetOf returns whether the given LookupOp will return a result set that is equivalent to or contained
	// within the calling LookupOp's result set.
	IsSupersetOf(LookupOp) (bool, error)
	// Union returns a new LookupOp that covers the entirety of both LookupOps' result sets. Returns nil if the result
	// sets do not overlap.
	Union(LookupOp) (LookupOp, error)
	// Intersection returns a new LookupOp that covers only the range where both LookupOps' result sets overlap.
	Intersection(LookupOp) (LookupOp, error)
	// ToReadRange returns this LookupOp as a ReadRange.
	ToReadRange() *noms.ReadRange
}

var (
	// ErrLookupOpUnhandledType is returned when a LookupOp has not been handled by the calling LookupOp.
	ErrLookupOpUnhandledType = errors.NewKind("unknown lookup op '%T'")
)
var (
	// alwaysContinueRangeCheck will allow the range to continue until the end is reached.
	alwaysContinueRangeCheck noms.InRangeCheck = func(tuple types.Tuple) (bool, error) {
		return true, nil
	}
	// neverContinueRangeCheck will immediately end.
	neverContinueRangeCheck noms.InRangeCheck = func(tuple types.Tuple) (bool, error) {
		return false, nil
	}
)

// NOTE: The key is a tuple representing a partial or full key. The more values that are in the key, the more specific
// it is. For example, a full key may have two values, such as [7,8] (ignoring tags). Iterating with the partial key [7]
// will match all keys starting with [7], including the full key [7,8]. If both operations involving the aforementioned
// keys are "Equals", then we can say that Equals[7,8] is a subset of Equals[7], since it has less potential matches
// (1 in this case since the key is full with two values). If we instead have Equals[6] and LessThan[7,8], then
// Equals[6] is a subset of LessThan[7,8], as all keys starting with the partial key [6] are less than any key matching
// [7,8]. We cannot, however, say that Equals[7] is a subset of LessThan[7,8], as Equals[7] matches both [7,8] and any
// key larger, such as [7,9]. They do overlap, from [7,min] to [7,8].
//
// Each comparison may have a small comment similar to the above, or an example of a comparison using the standard
// comparison signs (==, <, >, etc.) suffixed by a tuple.

// NOTE: The function names all contain underscores to make it visually easier to recognize each function considering
// the length of their names.

// Returns a new LookupOp representing the result set of all possible values.
func NewLookupOp_All(nbf *types.NomsBinFormat) LookupOp {
	return &all{
		nbf: nbf,
	}
}

// Returns a new LookupOp representing the empty result set.
func NewLookupOp_None(nbf *types.NomsBinFormat) LookupOp {
	return &none{
		nbf: nbf,
	}
}

// Returns a new LookupOp representing the result set (x == key).
func NewLookupOp_Equals(nbf *types.NomsBinFormat, key types.Tuple) LookupOp {
	return &equals{
		nbf: nbf,
		key: key,
	}
}

// Returns a new LookupOp representing the result set (x > key).
func NewLookupOp_GreaterThan(nbf *types.NomsBinFormat, key types.Tuple) LookupOp {
	return &greaterThan{
		nbf: nbf,
		key: key,
	}
}

// Returns a new LookupOp representing the result set (x >= key).
func NewLookupOp_GreaterThanOrEqual(nbf *types.NomsBinFormat, key types.Tuple) LookupOp {
	return &greaterThanOrEqual{
		nbf: nbf,
		key: key,
	}
}

// Returns a new LookupOp representing the result set (x < key).
func NewLookupOp_LessThan(nbf *types.NomsBinFormat, key types.Tuple) LookupOp {
	return &lessThan{
		nbf: nbf,
		key: key,
	}
}

// Returns a new LookupOp representing the result set (x <= key).
func NewLookupOp_LessThanOrEqual(nbf *types.NomsBinFormat, key types.Tuple) LookupOp {
	return &lessThanOrEqual{
		nbf: nbf,
		key: key,
	}
}

// Returns a new LookupOp representing the result set (lt > x > gt).
func NewLookupOp_GreaterThan_LessThan(nbf *types.NomsBinFormat, gt types.Tuple, lt types.Tuple) LookupOp {
	return &greaterThan_lessThan{
		nbf:   nbf,
		gtKey: gt,
		ltKey: lt,
	}
}

// Returns a new LookupOp representing the result set (lte >= x > gt).
func NewLookupOp_GreaterThan_LessThanOrEqual(nbf *types.NomsBinFormat, gt types.Tuple, lte types.Tuple) LookupOp {
	return &greaterThan_lessThanOrEqual{
		nbf:    nbf,
		gtKey:  gt,
		lteKey: lte,
	}
}

// Returns a new LookupOp representing the result set (lt > x >= gte).
func NewLookupOp_GreaterThanOrEqual_LessThan(nbf *types.NomsBinFormat, gte types.Tuple, lt types.Tuple) LookupOp {
	return &greaterThanOrEqual_lessThan{
		nbf:    nbf,
		gteKey: gte,
		ltKey:  lt,
	}
}

// Returns a new LookupOp representing the result set (lte >= x >= gte).
func NewLookupOp_GreaterThanOrEqual_LessThanOrEqual(nbf *types.NomsBinFormat, gte types.Tuple, lte types.Tuple) LookupOp {
	return &greaterThanOrEqual_lessThanOrEqual{
		nbf:    nbf,
		gteKey: gte,
		lteKey: lte,
	}
}
