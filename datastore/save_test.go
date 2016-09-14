// Copyright 2010 The Go Authors. All rights reserved.
// Copyright 2016 Morgan Conbere.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package datastore

import (
	"reflect"
	"testing"

	"google.golang.org/appengine/datastore"
)

func ptrToInt(x int) *int {
	return &x
}

func TestStructEncoding(t *testing.T) {
	testCases := []struct {
		in      interface{}
		want    []datastore.Property
		wantErr error
	}{
		{
			in: struct{ X bool }{X: true},
			want: []datastore.Property{{
				Name:  "X",
				Value: bool(true),
			}},
		},
		{
			in: struct{ X int }{X: -15},
			want: []datastore.Property{{
				Name:  "X",
				Value: int64(-15),
			}},
		},
		{
			in: struct{ X uint }{X: 15},
			want: []datastore.Property{{
				Name:  "X",
				Value: uint64(15),
			}},
		},
		{
			in: struct{ X float32 }{X: 1.5},
			want: []datastore.Property{{
				Name:  "X",
				Value: float64(1.5),
			}},
		},
		{
			in: struct{ X string }{X: "abc"},
			want: []datastore.Property{{
				Name:  "X",
				Value: string("abc"),
			}},
		},
		{
			in: struct{ X []bool }{X: []bool{true, false, true}},
			want: []datastore.Property{{
				Name:  "X",
				Value: []bool{true, false, true},
			}},
		},
		{
			in: struct{ X []int }{X: []int{1, 2, 3}},
			want: []datastore.Property{{
				Name:  "X",
				Value: []int64{1, 2, 3},
			}},
		},
		{
			in: struct{ X []string }{X: []string{"a", "b", "c"}},
			want: []datastore.Property{{
				Name:  "X",
				Value: []string{"a", "b", "c"},
			}},
		},
		{
			in: struct {
				X int
				Y string
			}{X: 15, Y: "a"},
			want: []datastore.Property{{
				Name:  "X",
				Value: int64(15),
			}, {
				Name:  "Y",
				Value: string("a"),
			}},
		},
		{
			in: struct {
				X int
				Y []string
			}{X: 15, Y: []string{"a"}},
			want: []datastore.Property{{
				Name:  "X",
				Value: int64(15),
			}, {
				Name:  "Y",
				Value: []string{"a"},
			}},
		},
		{
			in: struct {
				X int
				Y []string
			}{X: 15, Y: []string{"a", "b", "c"}},
			want: []datastore.Property{{
				Name:  "X",
				Value: int64(15),
			}, {
				Name:  "Y",
				Value: []string{"a", "b", "c"},
			}},
		},
		{
			in: struct {
				X *int
			}{X: ptrToInt(15)},
			want: []datastore.Property{{
				Name:  "X",
				Value: int64(15),
			}},
		},
	}
	for _, tc := range testCases {
		got, gotErr := SaveStruct(tc.in)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("SaveStruct(%v): got (%v, %v), want (%v, %v)", tc.in, got, gotErr, tc.want, tc.wantErr)
		}
	}
}
