// Copyright 2010 The Go Authors. All rights reserved.
// Copyright 2016 Morgan Conbere.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Package datastore implements an alternate interface
// for datastore encoding and decoding, providing the
// serialization promises of the encoding/json package
// while generating an appropriate []datastore.Property.
package datastore

import (
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

// tagOptions is the string following a comma in a struct field's "json"
// tag, or the empty string. It does not include the leading comma.
type tagOptions string

// parseTag splits a struct field's json tag into its name and
// comma-separated options.
func parseTag(tag string) (string, tagOptions) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], tagOptions(tag[idx+1:])
	}
	return tag, tagOptions("")
}

// Contains reports whether a comma-separated list of options
// contains a particular substr flag. substr must be surrounded by a
// string boundary or commas.
func (o tagOptions) Contains(optionName string) bool {
	if len(o) == 0 {
		return false
	}
	s := string(o)
	for s != "" {
		var next string
		i := strings.Index(s, ",")
		if i >= 0 {
			s, next = s[:i], s[i+1:]
		}
		if s == optionName {
			return true
		}
		s = next
	}
	return false
}

// SaveStruct returns a valid list of datastore.Property.
func SaveStruct(v interface{}) ([]datastore.Property, error) {
	e := &encodeState{}
	err := e.marshal(v, encOpts{prefix: "", noIndex: false, multiple: false})
	if err != nil {
		return nil, err
	}
	return e.props, nil
}

// UnsupportedTypeError is returned by Save when attempting
// to encode an unsupported type.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "datastore: unsupported type: " + e.Type.String()
}

// UnsupportedValueError is returned by Save when attempting
// to encode an unsupported value.
type UnsupportedValueError struct {
	Value reflect.Value
	Str   string
}

func (e *UnsupportedValueError) Error() string {
	return "datastore: unsupported value: " + e.Str
}

type SaverError struct {
	Type reflect.Type
	Err  error
}

func (e *SaverError) Error() string {
	return "datastore: error calling Save for type " + e.Type.String() + ": " + e.Err.Error()
}

// An encodeState encodes JSON into a bytes.Buffer.
type encodeState struct {
	props []datastore.Property
}

var encodeStatePool sync.Pool

func newEncodeState() *encodeState {
	if v := encodeStatePool.Get(); v != nil {
		e := v.(*encodeState)
		e.props = nil
		return e
	}
	return new(encodeState)
}

func (e *encodeState) marshal(v interface{}, opts encOpts) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			if s, ok := r.(string); ok {
				panic(s)
			}
			err = r.(error)
		}
	}()
	e.reflectValue(reflect.ValueOf(v), opts)
	return nil
}

func (e *encodeState) error(err error) {
	panic(err)
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func (e *encodeState) reflectValue(v reflect.Value, opts encOpts) {
	valueEncoder(v)(e, v, opts)
}

type encOpts struct {
	prefix   string
	noIndex  bool // If the property is indexed.
	multiple bool // If the struct is nested in a slice already.
}

type encoderFunc func(e *encodeState, v reflect.Value, opts encOpts)

var encoderCache struct {
	sync.RWMutex
	m map[reflect.Type]encoderFunc
}

func valueEncoder(v reflect.Value) encoderFunc {
	if !v.IsValid() {
		return invalidValueEncoder
	}
	return typeEncoder(v.Type())
}

func typeEncoder(t reflect.Type) encoderFunc {
	encoderCache.RLock()
	f := encoderCache.m[t]
	encoderCache.RUnlock()
	if f != nil {
		return f
	}

	// To deal with recursive types, populate the map with an
	// indirect func before we build it. This type waits on the
	// real func (f) to be ready and then calls it. This indirect
	// func is only used for recursive types.
	encoderCache.Lock()
	if encoderCache.m == nil {
		encoderCache.m = make(map[reflect.Type]encoderFunc)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	encoderCache.m[t] = func(e *encodeState, v reflect.Value, opts encOpts) {
		wg.Wait()
		f(e, v, opts)
	}
	encoderCache.Unlock()

	// Compute fields without lock.
	// Might duplicate effort but won't hold other computations back.
	f = newTypeEncoder(t, true)
	wg.Done()
	encoderCache.Lock()
	encoderCache.m[t] = f
	encoderCache.Unlock()
	return f
}

var (
	propertyLoadSaverType = reflect.TypeOf(new(datastore.PropertyLoadSaver)).Elem()

	datastoreKeyType = reflect.TypeOf((*datastore.Key)(nil)).Elem()
	blobKeyType      = reflect.TypeOf(appengine.BlobKey(""))
	geoPointType     = reflect.TypeOf(appengine.GeoPoint{})
	timeType         = reflect.TypeOf(time.Time{})
)

// newTypeEncoder constructs an encoderFunc for a type.
// The returned encoder only checks CanAddr when allowAddr is true.
func newTypeEncoder(t reflect.Type, allowAddr bool) encoderFunc {
	if t.Implements(propertyLoadSaverType) {
		return saverEncoder
	}
	if t.Kind() != reflect.Ptr && allowAddr {
		if reflect.PtrTo(t).Implements(propertyLoadSaverType) {
			return newCondAddrEncoder(addrSaverEncoder, newTypeEncoder(t, false))
		}
	}

	// Handle the special cases.
	switch t {
	case datastoreKeyType:
		return keyEncoder
	case timeType:
		return timeEncoder
	case blobKeyType:
		return blobKeyEncoder
	case geoPointType:
		return geoPointEncoder
	default:
		break
	}

	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintEncoder
	case reflect.Float32, reflect.Float64:
		return floatEncoder
	case reflect.String:
		return stringEncoder
	case reflect.Interface:
		return interfaceEncoder
	case reflect.Struct:
		return newStructEncoder(t)
	case reflect.Slice:
		return newSliceEncoder(t)
	case reflect.Array:
		return newArrayEncoder(t)
	case reflect.Ptr:
		return newPtrEncoder(t)
	default:
		return unsupportedTypeEncoder
	}
}

func invalidValueEncoder(e *encodeState, v reflect.Value, _ encOpts) {
	// Do nothing.
}

func saverEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		// Do nothing.
		return
	}
	pls := v.Interface().(datastore.PropertyLoadSaver)
	props, err := pls.Save()
	for _, p := range props {
		if p.Multiple && opts.multiple {
			e.error(&SaverError{v.Type(), err})
		}
		if opts.prefix != "" {
			p.Name = opts.prefix + "." + p.Name
		}
	}
	if err != nil {
		e.error(&SaverError{v.Type(), err})
		return
	}
	e.props = append(e.props, props...)
}

func addrSaverEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	va := v.Addr()
	if va.IsNil() {
		// Do nothing.
		return
	}
	saverEncoder(e, v, opts)
}

func boolEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v.Bool(),
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func intEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v.Int(),
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func uintEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v.Uint(),
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func floatEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v.Float(),
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func stringEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v.String(),
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func interfaceEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	if v.IsNil() {
		return
	}
	e.reflectValue(v.Elem(), opts)
}

func keyEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v,
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func timeEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v,
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func blobKeyEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v,
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func geoPointEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v,
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func unsupportedTypeEncoder(e *encodeState, v reflect.Value, _ encOpts) {
	e.error(&UnsupportedTypeError{v.Type()})
}

type structEncoder struct {
	fields    []field
	fieldEncs []encoderFunc
}

func (se *structEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	for i, f := range se.fields {
		fv := fieldByIndex(v, f.index)
		if !fv.IsValid() || isEmptyValue(fv) {
			continue
		}
		nopts := opts
		if nopts.prefix != "" {
			nopts.prefix = nopts.prefix + "." + f.name
		} else {
			nopts.prefix = f.name
		}
		nopts.noIndex = f.noIndex
		se.fieldEncs[i](e, fv, nopts)
	}
}

func newStructEncoder(t reflect.Type) encoderFunc {
	fields := cachedTypeFields(t)
	se := &structEncoder{
		fields:    fields,
		fieldEncs: make([]encoderFunc, len(fields)),
	}
	for i, f := range fields {
		se.fieldEncs[i] = typeEncoder(typeByIndex(t, f.index))
	}
	return se.encode
}

func encodeByteSlice(e *encodeState, v reflect.Value, opts encOpts) {
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    v.Bytes(),
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

// sliceEncoder just wraps an arrayEncoder, checking to make sure the value isn't nil.
type sliceEncoder struct {
	arrayEnc encoderFunc
}

func (se *sliceEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	if v.IsNil() {
		// Do nothing.
		return
	}
	se.arrayEnc(e, v, opts)
}

func newSliceEncoder(t reflect.Type) encoderFunc {
	// Byte slices get special treatment; arrays don't.
	if t.Elem().Kind() == reflect.Uint8 {
		p := reflect.PtrTo(t.Elem())
		if !p.Implements(propertyLoadSaverType) {
			return encodeByteSlice
		}
	}
	enc := &sliceEncoder{newArrayEncoder(t)}
	return enc.encode
}

type arrayEncoder struct {
	elemEnc encoderFunc
}

func (ae *arrayEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	ne := newEncodeState()
	nopts := encOpts{
		prefix:   opts.prefix,
		noIndex:  opts.noIndex,
		multiple: true,
	}
	n := v.Len()
	for i := 0; i < n; i++ {
		ae.elemEnc(ne, v.Index(i), nopts)
	}
	l := len(ne.props)
	if l == 0 {
		return
	}

	typ := reflect.TypeOf(ne.props[0].Value)
	slc := reflect.MakeSlice(reflect.SliceOf(typ), l, l)
	for i := 0; i < l; i++ {
		slc.Index(i).Set(reflect.ValueOf(ne.props[i].Value))
	}
	e.props = append(e.props, datastore.Property{
		Name:     opts.prefix,
		Value:    slc.Interface(),
		NoIndex:  opts.noIndex,
		Multiple: opts.multiple,
	})
}

func newArrayEncoder(t reflect.Type) encoderFunc {
	enc := &arrayEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

type ptrEncoder struct {
	elemEnc encoderFunc
}

func (pe *ptrEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	if v.IsNil() {
		return
	}
	pe.elemEnc(e, v.Elem(), opts)
}

func newPtrEncoder(t reflect.Type) encoderFunc {
	enc := &ptrEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

type condAddrEncoder struct {
	canAddrEnc, elseEnc encoderFunc
}

func (ce *condAddrEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	if v.CanAddr() {
		ce.canAddrEnc(e, v, opts)
	} else {
		ce.elseEnc(e, v, opts)
	}
}

// newCondAddrEncoder returns an encoder that checks whether its value
// CanAddr and delegates to canAddrEnc if so, else to elseEnc.
func newCondAddrEncoder(canAddrEnc, elseEnc encoderFunc) encoderFunc {
	enc := &condAddrEncoder{canAddrEnc: canAddrEnc, elseEnc: elseEnc}
	return enc.encode
}

func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:<=>?@[]^_{|}~ ", c):
			// Backslash and quote chars are reserved, but
			// otherwise any punctuation chars are allowed
			// in a tag name.
		default:
			if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
				return false
			}
		}
	}
	return true
}

func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return reflect.Value{}
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

func typeByIndex(t reflect.Type, index []int) reflect.Type {
	for _, i := range index {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		t = t.Field(i).Type
	}
	return t
}

// A field represents a single field found in a struct.
type field struct {
	name string

	tag     bool
	index   []int
	typ     reflect.Type
	noIndex bool
}

// byName sorts field by name, breaking ties with depth,
// then breaking ties with "name came from json tag", then
// breaking ties with index sequence.
type byName []field

func (x byName) Len() int { return len(x) }

func (x byName) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byName) Less(i, j int) bool {
	if x[i].name != x[j].name {
		return x[i].name < x[j].name
	}
	if len(x[i].index) != len(x[j].index) {
		return len(x[i].index) < len(x[j].index)
	}
	if x[i].tag != x[j].tag {
		return x[i].tag
	}
	return byIndex(x).Less(i, j)
}

// byIndex sorts field by index sequence.
type byIndex []field

func (x byIndex) Len() int { return len(x) }

func (x byIndex) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byIndex) Less(i, j int) bool {
	for k, xik := range x[i].index {
		if k >= len(x[j].index) {
			return false
		}
		if xik != x[j].index[k] {
			return xik < x[j].index[k]
		}
	}
	return len(x[i].index) < len(x[j].index)
}

// typeFields returns a list of fields that JSON should recognize for the given type.
// The algorithm is breadth-first search over the set of structs to include - the top struct
// and then any reachable anonymous structs.
func typeFields(t reflect.Type) []field {
	// Anonymous fields to explore at the current level and the next.
	current := []field{}
	next := []field{{typ: t}}

	// Count of queued names for current level and the next.
	count := map[reflect.Type]int{}
	nextCount := map[reflect.Type]int{}

	// Types already visited at an earlier level.
	visited := map[reflect.Type]bool{}

	// Fields found.
	var fields []field

	for len(next) > 0 {
		current, next = next, current[:0]
		count, nextCount = nextCount, map[reflect.Type]int{}

		for _, f := range current {
			if visited[f.typ] {
				continue
			}
			visited[f.typ] = true

			// Scan f.typ for fields to include.
			for i := 0; i < f.typ.NumField(); i++ {
				sf := f.typ.Field(i)
				if sf.PkgPath != "" && !sf.Anonymous { // unexported
					continue
				}
				tag := sf.Tag.Get("datastore")
				if tag == "-" {
					continue
				}
				name, opts := parseTag(tag)
				if !isValidTag(name) {
					name = ""
				}
				index := make([]int, len(f.index)+1)
				copy(index, f.index)
				index[len(f.index)] = i

				ft := sf.Type
				if ft.Name() == "" && ft.Kind() == reflect.Ptr {
					// Follow pointer.
					ft = ft.Elem()
				}

				// Record found field and index sequence.
				if name != "" || !sf.Anonymous || ft.Kind() != reflect.Struct {
					tagged := name != ""
					if name == "" {
						name = sf.Name
					}
					fields = append(fields, field{
						name:    name,
						tag:     tagged,
						index:   index,
						typ:     ft,
						noIndex: opts.Contains("noindex"),
					})
					if count[f.typ] > 1 {
						// If there were multiple instances, add a second,
						// so that the annihilation code will see a duplicate.
						// It only cares about the distinction between 1 or 2,
						// so don't bother generating any more copies.
						fields = append(fields, fields[len(fields)-1])
					}
					continue
				}

				// Record new anonymous struct to explore in next round.
				nextCount[ft]++
				if nextCount[ft] == 1 {
					next = append(next, field{name: ft.Name(), index: index, typ: ft})
				}
			}
		}
	}

	sort.Sort(byName(fields))

	// Delete all fields that are hidden by the Go rules for embedded fields,
	// except that fields with JSON tags are promoted.

	// The fields are sorted in primary order of name, secondary order
	// of field index length. Loop over names; for each name, delete
	// hidden fields by choosing the one dominant field that survives.
	out := fields[:0]
	for advance, i := 0, 0; i < len(fields); i += advance {
		// One iteration per name.
		// Find the sequence of fields with the name of this first field.
		fi := fields[i]
		name := fi.name
		for advance = 1; i+advance < len(fields); advance++ {
			fj := fields[i+advance]
			if fj.name != name {
				break
			}
		}
		if advance == 1 { // Only one field with this name
			out = append(out, fi)
			continue
		}
		dominant, ok := dominantField(fields[i : i+advance])
		if ok {
			out = append(out, dominant)
		}
	}

	fields = out
	sort.Sort(byIndex(fields))

	return fields
}

// dominantField looks through the fields, all of which are known to
// have the same name, to find the single field that dominates the
// others using Go's embedding rules, modified by the presence of
// JSON tags. If there are multiple top-level fields, the boolean
// will be false: This condition is an error in Go and we skip all
// the fields.
func dominantField(fields []field) (field, bool) {
	// The fields are sorted in increasing index-length order. The winner
	// must therefore be one with the shortest index length. Drop all
	// longer entries, which is easy: just truncate the slice.
	length := len(fields[0].index)
	tagged := -1 // Index of first tagged field.
	for i, f := range fields {
		if len(f.index) > length {
			fields = fields[:i]
			break
		}
		if f.tag {
			if tagged >= 0 {
				// Multiple tagged fields at the same level: conflict.
				// Return no field.
				return field{}, false
			}
			tagged = i
		}
	}
	if tagged >= 0 {
		return fields[tagged], true
	}
	// All remaining fields have the same length. If there's more than one,
	// we have a conflict (two fields named "X" at the same level) and we
	// return no field.
	if len(fields) > 1 {
		return field{}, false
	}
	return fields[0], true
}

var fieldCache struct {
	value atomic.Value // map[reflect.Type][]field
	mu    sync.Mutex   // used only by writers
}

// cachedTypeFields is like typeFields but uses a cache to avoid repeated work.
func cachedTypeFields(t reflect.Type) []field {
	m, _ := fieldCache.value.Load().(map[reflect.Type][]field)
	f := m[t]
	if f != nil {
		return f
	}

	// Compute fields without lock.
	// Might duplicate effort but won't hold other computations back.
	f = typeFields(t)
	if f == nil {
		f = []field{}
	}

	fieldCache.mu.Lock()
	m, _ = fieldCache.value.Load().(map[reflect.Type][]field)
	newM := make(map[reflect.Type][]field, len(m)+1)
	for k, v := range m {
		newM[k] = v
	}
	newM[t] = f
	fieldCache.value.Store(newM)
	fieldCache.mu.Unlock()
	return f
}
