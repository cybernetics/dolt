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

package sqle

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookupops"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx       DoltIndex
	lookupOps []lookupops.LookupOp
}

var _ sql.MergeableIndexLookup = (*doltIndexLookup)(nil)

func (il *doltIndexLookup) String() string {
	// TODO: this could be expanded with additional info (like the expression used to create the index lookup)
	return fmt.Sprintf("doltIndexLookup:%s", il.idx.ID())
}

// IsMergeable implements sql.MergeableIndexLookup
func (il *doltIndexLookup) IsMergeable(indexLookup sql.IndexLookup) bool {
	otherIl, ok := indexLookup.(*doltIndexLookup)
	if !ok {
		return false
	}
	return il.idx == otherIl.idx
}

// Intersection implements sql.MergeableIndexLookup
//TODO: change signature to accept errors
func (il *doltIndexLookup) Intersection(indexLookups ...sql.IndexLookup) sql.IndexLookup {
	lookupOp := lookupops.NewLookupOp_All(il.idx.TableData().Format())
	var err error
	for _, ilLookupOp := range il.lookupOps {
		lookupOp, err = lookupOp.Intersection(ilLookupOp)
		if err != nil {
			panic(err)
		}
	}
	for _, indexLookup := range indexLookups {
		otherIl, ok := indexLookup.(*doltIndexLookup)
		if !ok {
			panic(fmt.Errorf("failed to intersect sql.IndexLookup with type '%T'", indexLookup))
		}
		for _, ilLookupOp := range otherIl.lookupOps {
			lookupOp, err = lookupOp.Intersection(ilLookupOp)
			if err != nil {
				panic(err)
			}
		}
	}
	return &doltIndexLookup{
		idx:       il.idx,
		lookupOps: []lookupops.LookupOp{lookupOp},
	}
}

// Union implements sql.MergeableIndexLookup
//TODO: change signature to accept errors
func (il *doltIndexLookup) Union(indexLookups ...sql.IndexLookup) sql.IndexLookup {
	var lookupOps []lookupops.LookupOp
	if len(il.lookupOps) == 0 {
		lookupOps = []lookupops.LookupOp{lookupops.NewLookupOp_None(il.idx.TableData().Format())}
	} else {
		lookupOps = make([]lookupops.LookupOp, len(il.lookupOps))
		copy(lookupOps, il.lookupOps)
	}
	for _, indexLookup := range indexLookups {
		otherIl, ok := indexLookup.(*doltIndexLookup)
		if !ok {
			panic(fmt.Errorf("failed to union sql.IndexLookup with type '%T'", indexLookup))
		}
		lookupOps = append(lookupOps, otherIl.lookupOps...)
	}
	for op1Index := 0; op1Index < len(lookupOps); op1Index++ {
		for op2Index := op1Index + 1; op2Index < len(lookupOps); op2Index++ {
			newLookupOp, err := lookupOps[op1Index].Union(lookupOps[op2Index])
			if err != nil {
				panic(err)
			}
			// If the LookupOps overlapped then we need to remove the current ones from the slice.
			if newLookupOp != nil {
				// Remove the 2nd LookupOp from the slice
				lookupOps = append(lookupOps[:op2Index], lookupOps[op2Index+1:]...)
				// Replace the 1st LookupOp with the new one
				lookupOps[op1Index] = newLookupOp
				// Decrement the 1st LookupOp index so that we'll start at the new LookupOp on the outer loop.
				// Any previous LookupOps on the outer loop did not overlap with any other LookupOps, so they will also
				// not overlap with any new LookupOps.
				op1Index--
				break
			}
		}
	}
	return &doltIndexLookup{
		idx:       il.idx,
		lookupOps: lookupOps,
	}
}

// Difference implements sql.MergeableIndexLookup
func (il *doltIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("MergeableIndexLookup Difference unused at the time of implementation")
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	readRanges := make([]*noms.ReadRange, len(il.lookupOps))
	for i, lookupOp := range il.lookupOps {
		readRanges[i] = lookupOp.ToReadRange()
	}
	return &indexLookupRowIterAdapter{
		idx: il.idx,
		keyIter: &doltIndexKeyIter{
			indexMapIter: noms.NewNomsRangeReader(il.idx.IndexSchema(), il.idx.IndexRowData(), readRanges),
		},
		ctx: ctx,
	}, nil
}

type doltIndexKeyIter struct {
	indexMapIter table.TableReadCloser
}

var _ IndexLookupKeyIterator = (*doltIndexKeyIter)(nil)

func (iter *doltIndexKeyIter) NextKey(ctx *sql.Context) (row.TaggedValues, error) {
	indexRow, err := iter.indexMapIter.ReadRow(ctx)
	if err != nil {
		return nil, err
	}
	return row.GetTaggedVals(indexRow)
}
