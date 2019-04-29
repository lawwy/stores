package sql4

import "reflect"

func StructFields(t reflect.Type, need StructFieldFilter) []reflect.StructField {
	ff := []reflect.StructField{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if need != nil && need(f) {
			ff = append(ff, f)
		}
	}
	return ff
}

func FieldsValues(v reflect.Value, fields []reflect.StructField) []interface{} {
	vv := []interface{}{}
	for _, f := range fields {
		vv = append(vv, v.FieldByName(f.Name).Interface())
	}
	return vv
}

func FieldsPointers(v reflect.Value, fields []reflect.StructField) []interface{} {
	vv := []interface{}{}
	for _, f := range fields {
		vv = append(vv, v.FieldByName(f.Name).Addr().Interface())
	}
	return vv
}

func MakeStubs(v string, l int) []string {
	stubs := make([]string, l)
	for i := 0; i < l; i++ {
		stubs[i] = "?"
	}
	return stubs
}

func MapStrings(ss []string, mapper func(string) string) []string {
	nss := make([]string, len(ss))
	for i, s := range ss {
		nss[i] = mapper(s)
	}
	return nss
}

func ModelTypeAndValue(model interface{}) (reflect.Type, reflect.Value) {
	t := reflect.TypeOf(model).Elem()
	v := reflect.Indirect(reflect.ValueOf(model))
	return t, v
}

func ListTypeAndValue(list interface{}) (reflect.Type, reflect.Value) {
	lv := reflect.ValueOf(list).Elem()             //切片值(非指针)
	t := reflect.TypeOf(list).Elem().Elem().Elem() //切片成员类型(非指针)
	return t, lv
}

func isEmptyPointer(model interface{}) bool {
	return reflect.ValueOf(model).IsNil()
}

func B2S(bs []uint8) string {
	b := make([]byte, len(bs))
	for i, v := range bs {
		b[i] = byte(v)
	}
	return string(b)
}
