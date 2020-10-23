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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestLookupOpImplementationCoverage(t *testing.T) {
	testKey, err := types.NewTuple(types.Format_Default, types.Uint(0))
	require.NoError(t, err)
	lookupOps := []LookupOp{
		// Every op should go here, as it will verify that each one handles all others
		NewLookupOp_All(types.Format_Default),
		NewLookupOp_None(types.Format_Default),
		NewLookupOp_Equals(types.Format_Default, testKey),
		NewLookupOp_GreaterThan(types.Format_Default, testKey),
		NewLookupOp_GreaterThanOrEqual(types.Format_Default, testKey),
		NewLookupOp_LessThan(types.Format_Default, testKey),
		NewLookupOp_LessThanOrEqual(types.Format_Default, testKey),
		NewLookupOp_GreaterThan_LessThan(types.Format_Default, testKey, testKey),
		NewLookupOp_GreaterThan_LessThanOrEqual(types.Format_Default, testKey, testKey),
		NewLookupOp_GreaterThanOrEqual_LessThan(types.Format_Default, testKey, testKey),
		NewLookupOp_GreaterThanOrEqual_LessThanOrEqual(types.Format_Default, testKey, testKey),
	}
	for _, op1 := range lookupOps {
		for _, op2 := range lookupOps {
			t.Run(fmt.Sprintf("%T IsSubsetOf %T", op1, op2), func(t *testing.T) {
				_, err := op1.IsSubsetOf(op2)
				assert.False(t, ErrLookupOpUnhandledType.Is(err))
			})
			t.Run(fmt.Sprintf("%T IsSupersetOf %T", op1, op2), func(t *testing.T) {
				_, err := op1.IsSupersetOf(op2)
				assert.False(t, ErrLookupOpUnhandledType.Is(err))
			})
			t.Run(fmt.Sprintf("%T Union %T", op1, op2), func(t *testing.T) {
				_, err := op1.Union(op2)
				assert.False(t, ErrLookupOpUnhandledType.Is(err))
			})
			t.Run(fmt.Sprintf("%T Intersection %T", op1, op2), func(t *testing.T) {
				_, err := op1.Intersection(op2)
				assert.False(t, ErrLookupOpUnhandledType.Is(err))
			})
		}
	}
}
