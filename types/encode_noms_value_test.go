package types

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func TestWritePrimitives(t *testing.T) {
	assert := assert.New(t)

	f := func(k NomsKind, v Value, ex interface{}) {
		cs := chunks.NewMemoryStore()
		w := newJsonArrayWriter(cs)
		w.writeTopLevelValue(v)
		assert.EqualValues([]interface{}{k, ex}, w.toArray())
	}

	f(BoolKind, Bool(true), true)
	f(BoolKind, Bool(false), false)

	f(UInt8Kind, UInt8(0), uint8(0))
	f(UInt16Kind, UInt16(0), uint16(0))
	f(UInt32Kind, UInt32(0), uint32(0))
	f(UInt64Kind, UInt64(0), uint64(0))
	f(Int8Kind, Int8(0), int8(0))
	f(Int16Kind, Int16(0), int16(0))
	f(Int32Kind, Int32(0), int32(0))
	f(Int64Kind, Int64(0), int64(0))
	f(Float32Kind, Float32(0), float32(0))
	f(Float64Kind, Float64(0), float64(0))

	f(StringKind, NewString("hi"), "hi")

	blob := NewMemoryBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	f(BlobKind, blob, "AAE=")
}

func TestWriteList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeCompoundType(ListKind, MakePrimitiveType(Int32Kind))
	v := NewTypedList(typ, Int32(0), Int32(1), Int32(2), Int32(3))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, Int32Kind, []interface{}{int32(0), int32(1), int32(2), int32(3)}}, w.toArray())
}

func TestWriteListOfList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	it := MakeCompoundType(ListKind, MakePrimitiveType(Int16Kind))
	typ := MakeCompoundType(ListKind, it)
	l1 := NewTypedList(it, Int16(0))
	l2 := NewTypedList(it, Int16(1), Int16(2), Int16(3))
	v := NewTypedList(typ, l1, l2)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ListKind, Int16Kind,
		[]interface{}{[]interface{}{int16(0)}, []interface{}{int16(1), int16(2), int16(3)}}}, w.toArray())
}

func TestWriteSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeCompoundType(SetKind, MakePrimitiveType(UInt32Kind))
	v := NewTypedSet(typ, UInt32(3), UInt32(1), UInt32(2), UInt32(0))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, UInt32Kind, []interface{}{uint32(0), uint32(1), uint32(2), uint32(3)}}, w.toArray())
}

func TestWriteSetOfSet(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	st := MakeCompoundType(SetKind, MakePrimitiveType(Int32Kind))
	typ := MakeCompoundType(SetKind, st)
	v := NewTypedSet(typ, NewTypedSet(st, Int32(0)), NewTypedSet(st, Int32(1), Int32(2), Int32(3)))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{SetKind, SetKind, Int32Kind, []interface{}{[]interface{}{int32(1), int32(2), int32(3)}, []interface{}{int32(0)}}}, w.toArray())
}

func TestWriteMap(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeCompoundType(MapKind, MakePrimitiveType(StringKind), MakePrimitiveType(BoolKind))
	v := NewTypedMap(typ, NewString("a"), Bool(false), NewString("b"), Bool(true))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	// The order of the elements is based on the order defined by OrderedValue.
	assert.EqualValues([]interface{}{MapKind, StringKind, BoolKind, []interface{}{"a", false, "b", true}}, w.toArray())
}

func TestWriteMapOfMap(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	kt := MakeCompoundType(MapKind, MakePrimitiveType(StringKind), MakePrimitiveType(Int64Kind))
	vt := MakeCompoundType(SetKind, MakePrimitiveType(BoolKind))
	typ := MakeCompoundType(MapKind, kt, vt)
	v := NewTypedMap(typ, NewTypedMap(kt, NewString("a"), Int64(0)), NewTypedSet(vt, Bool(true)))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MapKind, MapKind, StringKind, Int64Kind, SetKind, BoolKind, []interface{}{[]interface{}{"a", int64(0)}, []interface{}{true}}}, w.toArray())
}

func TestWriteCompoundBlob(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	r1 := ref.Parse("sha1-0000000000000000000000000000000000000001")
	r2 := ref.Parse("sha1-0000000000000000000000000000000000000002")
	r3 := ref.Parse("sha1-0000000000000000000000000000000000000003")

	v := newCompoundBlob([]metaTuple{{r1, UInt64(20)}, {r2, UInt64(40)}, {r3, UInt64(60)}}, cs)
	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)

	// the order of the elements is based on the ref of the value.
	assert.EqualValues([]interface{}{MetaSequenceKind, BlobKind, []interface{}{r1.String(), uint64(20), r2.String(), uint64(40), r3.String(), uint64(60)}}, w.toArray())
}

func TestWriteEmptyStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructType("S", []Field{}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, nil)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0)}, w.toArray())
}

func TestWriteStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int8Kind), false},
		Field{"b", MakePrimitiveType(BoolKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), true}, w.toArray())
}

func TestWriteStructOptionalField(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int8Kind), true},
		Field{"b", MakePrimitiveType(BoolKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), true, int8(42), true}, w.toArray())

	v = NewStruct(typ, typeDef, structData{"b": Bool(true)})

	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), false, true}, w.toArray())
}

func TestWriteStructWithUnion(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int8Kind), false},
	}, Choices{
		Field{"b", MakePrimitiveType(BoolKind), false},
		Field{"s", MakePrimitiveType(StringKind), false},
	})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	v := NewStruct(typ, typeDef, structData{"x": Int8(42), "s": NewString("hi")})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), uint32(1), "hi"}, w.toArray())

	v = NewStruct(typ, typeDef, structData{"x": Int8(42), "b": Bool(true)})

	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), int8(42), uint32(0), true}, w.toArray())
}

func TestWriteStructWithList(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructType("S", []Field{
		Field{"l", MakeCompoundType(ListKind, MakePrimitiveType(StringKind)), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	v := NewStruct(typ, typeDef, structData{"l": NewList(NewString("a"), NewString("b"))})
	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), []interface{}{"a", "b"}}, w.toArray())

	v = NewStruct(typ, typeDef, structData{"l": NewList()})
	w = newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), []interface{}{}}, w.toArray())
}

func TestWriteStructWithStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	s2TypeDef := MakeStructType("S2", []Field{
		Field{"x", MakePrimitiveType(Int32Kind), false},
	}, Choices{})
	sTypeDef := MakeStructType("S", []Field{
		Field{"s", MakeType(ref.Ref{}, 0), false},
	}, Choices{})
	pkg := NewPackage([]Type{s2TypeDef, sTypeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	s2Type := MakeType(pkgRef, 0)
	sType := MakeType(pkgRef, 1)

	v := NewStruct(sType, sTypeDef, structData{"s": NewStruct(s2Type, s2TypeDef, structData{"x": Int32(42)})})
	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(1), int32(42)}, w.toArray())
}

func TestWriteStructWithBlob(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructType("S", []Field{
		Field{"b", MakePrimitiveType(BlobKind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)
	b := NewMemoryBlob(bytes.NewBuffer([]byte{0x00, 0x01}))
	v := NewStruct(typ, typeDef, structData{"b": b})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), "AAE="}, w.toArray())
}

func TestWriteEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{
		MakeEnumType("E", "a", "b", "c")}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(Enum{1, typ})
	assert.EqualValues([]interface{}{UnresolvedKind, pkgRef.String(), int16(0), uint32(1)}, w.toArray())
}

func TestWriteListOfEnum(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{
		MakeEnumType("E", "a", "b", "c")}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	et := MakeType(pkgRef, 0)
	typ := MakeCompoundType(ListKind, et)
	v := NewTypedList(typ, Enum{0, et}, Enum{1, et}, Enum{2, et})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, UnresolvedKind, pkgRef.String(), int16(0), []interface{}{uint32(0), uint32(1), uint32(2)}}, w.toArray())
}

func TestWriteListOfValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))
	blob := NewMemoryBlob(bytes.NewBuffer([]byte{0x01}))
	v := NewTypedList(typ,
		Bool(true),
		UInt8(1),
		UInt16(1),
		UInt32(1),
		UInt64(1),
		Int8(1),
		Int16(1),
		Int32(1),
		Int64(1),
		Float32(1),
		Float64(1),
		NewString("hi"),
		blob,
	)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)

	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{
		BoolKind, true,
		UInt8Kind, uint8(1),
		UInt16Kind, uint16(1),
		UInt32Kind, uint32(1),
		UInt64Kind, uint64(1),
		Int8Kind, int8(1),
		Int16Kind, int16(1),
		Int32Kind, int32(1),
		Int64Kind, int64(1),
		Float32Kind, float32(1),
		Float64Kind, float64(1),
		StringKind, "hi",
		BlobKind, "AQ==",
	}}, w.toArray())
}

func TestWriteListOfValueWithStruct(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typeDef := MakeStructType("S", []Field{
		Field{"x", MakePrimitiveType(Int32Kind), false},
	}, Choices{})
	pkg := NewPackage([]Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	listType := MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))
	structType := MakeType(pkgRef, 0)
	v := NewTypedList(listType, NewStruct(structType, typeDef, structData{"x": Int32(42)}))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{UnresolvedKind, pkgRef.String(), int16(0), int32(42)}}, w.toArray())
}

func TestWriteListOfValueWithType(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	pkg := NewPackage([]Type{
		MakeStructType("S", []Field{
			Field{"x", MakePrimitiveType(Int32Kind), false},
		}, Choices{})}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)

	typ := MakeCompoundType(ListKind, MakePrimitiveType(ValueKind))
	v := NewTypedList(typ,
		Bool(true),
		MakePrimitiveType(Int32Kind),
		MakePrimitiveType(TypeKind),
		MakeType(pkgRef, 0),
	)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, ValueKind, []interface{}{
		BoolKind, true,
		TypeKind, Int32Kind,
		TypeKind, TypeKind,
		TypeKind, UnresolvedKind, pkgRef.String(), int16(0),
	}}, w.toArray())
}

type testRef struct {
	Value
	t Type
}

func (r testRef) Type() Type {
	return r.t
}

func (r testRef) TargetRef() ref.Ref {
	return r.Value.(Ref).TargetRef()
}

func TestWriteRef(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeCompoundType(RefKind, MakePrimitiveType(UInt32Kind))
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := NewRef(r)

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(testRef{Value: v, t: typ})
	assert.EqualValues([]interface{}{RefKind, UInt32Kind, r.String()}, w.toArray())
}

func TestWriteTypeValue(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	test := func(expected []interface{}, v Type) {
		w := newJsonArrayWriter(cs)
		w.writeTopLevelValue(v)
		assert.EqualValues(expected, w.toArray())
	}

	test([]interface{}{TypeKind, Int32Kind}, MakePrimitiveType(Int32Kind))
	test([]interface{}{TypeKind, ListKind, []interface{}{BoolKind}},
		MakeCompoundType(ListKind, MakePrimitiveType(BoolKind)))
	test([]interface{}{TypeKind, MapKind, []interface{}{BoolKind, StringKind}},
		MakeCompoundType(MapKind, MakePrimitiveType(BoolKind), MakePrimitiveType(StringKind)))
	test([]interface{}{TypeKind, EnumKind, "E", []interface{}{"a", "b", "c"}},
		MakeEnumType("E", "a", "b", "c"))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"x", Int16Kind, false, "v", ValueKind, true}, []interface{}{}},
		MakeStructType("S", []Field{
			Field{"x", MakePrimitiveType(Int16Kind), false},
			Field{"v", MakePrimitiveType(ValueKind), true},
		}, Choices{}))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{}, []interface{}{"x", Int16Kind, false, "v", ValueKind, false}},
		MakeStructType("S", []Field{}, Choices{
			Field{"x", MakePrimitiveType(Int16Kind), false},
			Field{"v", MakePrimitiveType(ValueKind), false},
		}))

	pkgRef := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	test([]interface{}{TypeKind, UnresolvedKind, pkgRef.String(), int16(123)},
		MakeType(pkgRef, 123))

	test([]interface{}{TypeKind, StructKind, "S", []interface{}{"e", UnresolvedKind, pkgRef.String(), int16(123), false, "x", Int64Kind, false}, []interface{}{}},
		MakeStructType("S", []Field{
			Field{"e", MakeType(pkgRef, 123), false},
			Field{"x", MakePrimitiveType(Int64Kind), false},
		}, Choices{}))

	test([]interface{}{TypeKind, UnresolvedKind, ref.Ref{}.String(), int16(-1), "ns", "n"},
		MakeUnresolvedType("ns", "n"))
}

func TestWriteListOfTypes(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	typ := MakeCompoundType(ListKind, MakePrimitiveType(TypeKind))
	v := NewTypedList(typ, MakePrimitiveType(BoolKind), MakeEnumType("E", "a", "b", "c"), MakePrimitiveType(StringKind))

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{ListKind, TypeKind, []interface{}{BoolKind, EnumKind, "E", []interface{}{"a", "b", "c"}, StringKind}}, w.toArray())
}

func TestWritePackage(t *testing.T) {
	cs := chunks.NewMemoryStore()
	pkg := NewPackage([]Type{
		MakeStructType("EnumStruct",
			[]Field{
				Field{"hand", MakeType(ref.Ref{}, 1), false},
			},
			Choices{},
		),
		MakeEnumType("Handedness", "right", "left", "switch"),
	}, []ref.Ref{})

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(pkg)

	// struct Package {
	// 	Dependencies: Set(Ref(Package))
	// 	Types: List(Type)
	// }

	exp := []interface{}{
		PackageKind,
		[]interface{}{
			StructKind, "EnumStruct", []interface{}{
				"hand", UnresolvedKind, "sha1-0000000000000000000000000000000000000000", int16(1), false,
			}, []interface{}{},
			EnumKind, "Handedness", []interface{}{"right", "left", "switch"},
		},
		[]interface{}{}, // Dependencies
	}

	assert.EqualValues(t, exp, w.toArray())
}

func TestWritePackage2(t *testing.T) {
	assert := assert.New(t)
	cs := chunks.NewMemoryStore()

	setTref := MakeCompoundType(SetKind, MakePrimitiveType(UInt32Kind))
	r := ref.Parse("sha1-0123456789abcdef0123456789abcdef01234567")
	v := Package{[]Type{setTref}, []ref.Ref{r}, &ref.Ref{}}

	w := newJsonArrayWriter(cs)
	w.writeTopLevelValue(v)
	assert.EqualValues([]interface{}{PackageKind, []interface{}{SetKind, []interface{}{UInt32Kind}}, []interface{}{r.String()}}, w.toArray())
}
