package sql4

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
)

var (
	ProtoTimestampType = reflect.TypeOf(&timestamp.Timestamp{})
	OriginTimeType     = reflect.TypeOf(time.Now())
)

type Store interface {
	Write(interface{}, interface{}) error
	Read(interface{}, interface{}) error // key, out set value interface{}
	ReadAll(interface{}) error
}

//TODO:tablename,tranlater...
type Descriptor struct {
	table       string
	key         string
	autoKey     string
	backend     *SqlBackend
	fieldFilter StructFieldFilter
}

func (s *SqlBackend) Read(key interface{}, model interface{}) error {
	mdef := s.ModelDesriptor(model)
	m, err := mdef.Record(key)
	if err != nil {
		return err
	}
	if m == nil {
		return sql.ErrNoRows
	}
	err = mdef.RecordToModel(m, model)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBackend) ReadAll(mlist interface{}) error {
	t, lv := ListTypeAndValue(mlist)
	mdef := s.ModelDesriptor(reflect.New(t).Interface())
	rr, err := mdef.Records()
	if err != nil {
		return err
	}
	for _, r := range rr {
		vptr := reflect.New(t)
		err = mdef.RecordToModel(r, vptr.Interface())
		if err != nil {
			return nil
		}
		lv.Set(reflect.Append(lv, vptr))
	}
	return nil
}

func (s *SqlBackend) Write(key interface{}, model interface{}) error {
	mdef := s.ModelDesriptor(model)
	if isEmptyPointer(model) {
		return mdef.RemoveRecord(key)
	}
	r, err := mdef.Record(key)
	if err != nil {
		return err
	}
	if r == nil {
		r, err = mdef.NewRecord(key)
		if err != nil {
			return err
		}
	}
	err = mdef.ModelToRecord(model, r)
	if err != nil {
		return err
	}
	return mdef.UpdateRecord(r)
}

func (s *SqlBackend) ModelDesriptor(model interface{}) *Descriptor {
	t, _ := ModelTypeAndValue(model)
	return &Descriptor{
		table:       t.Name(),
		key:         "Id",
		backend:     s,
		fieldFilter: ProtoFieldFilter,
		autoKey:     "autoId",
	}
}

// func (def *Descriptor) Record(key interface{}) (map[string]interface{}, error) {
// 	query := fmt.Sprintf("select * from %s where %s = ?", def.table, def.key)
// 	rows, err := def.backend.DB.Queryx(query, key)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
// 		m := make(map[string]interface{})
// 		err = rows.MapScan(m)
// 		if err != nil {
// 			if err == sql.ErrNoRows {
// 				return nil, nil
// 			}
// 			return nil, err
// 		}
// 		return m, nil
// 		// break
// 	}
// 	return nil, nil
// }

func (def *Descriptor) Record(key interface{}) (map[string]interface{}, error) {
	query := fmt.Sprintf("select * from %s where %s = ?", def.table, def.key)
	m := make(map[string]interface{})
	err := def.backend.DB.QueryRowx(query, key).MapScan(m)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return m, nil
}

func (def *Descriptor) Records() ([]map[string]interface{}, error) {
	query := fmt.Sprintf("select * from %s", def.table)
	// rows, err := def.backend.DB.Queryx(query, def.table)
	st, err := def.backend.DB.Preparex(query)
	if err != nil {
		return nil, err
	}
	rows, err := st.Queryx()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mm := []map[string]interface{}{}
	for rows.Next() {
		m := make(map[string]interface{})
		err := rows.MapScan(m)
		if err != nil {
			return nil, err
		}
		mm = append(mm, m)
	}
	return mm, nil
}

//UNSTABLE
func (def *Descriptor) NewRecord(key interface{}) (map[string]interface{}, error) {
	cmd := fmt.Sprintf("insert into %s (%s) values (?)", def.table, def.key)
	_, err := def.backend.DB.Exec(cmd, key)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	m[def.key] = key
	return m, nil
}

func (def *Descriptor) RecordToModel(raw map[string]interface{}, model interface{}) error {
	mt, mv := ModelTypeAndValue(model)
	for k, v := range raw {
		fv := mv.FieldByName(k)
		ft, _ := mt.FieldByName(k)
		if !def.fieldFilter(ft) {
			continue
		}
		// fmt.Println(fv.Kind())
		switch fv.Kind() {
		//TODO:proto中的timestamp类型转换
		case reflect.String:
			_v := v.([]uint8)
			fv.SetString(B2S(_v))
		case reflect.Float32, reflect.Float64:
			fv.SetFloat(v.(float64))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fv.SetInt(v.(int64))
		case reflect.Bool:
			_v := v.(int64)
			if _v == 0 {
				fv.SetBool(false)
			}
			fv.SetBool(true)
		case reflect.Struct:
			if reflect.TypeOf(v).AssignableTo(fv.Type()) {
				fv.Set(reflect.ValueOf(v))
				// break
			}
		case reflect.Ptr:
			if ft.Type == ProtoTimestampType {
				_time, _ := reflect.ValueOf(v).Interface().(time.Time)
				_v, _ := ptypes.TimestampProto(_time)
				fv.Set(reflect.ValueOf(_v))
				break
			}
		}

	}
	return nil
}

//TODO:需过滤字段
func (def *Descriptor) ModelToRecord(model interface{}, raw map[string]interface{}) error {
	t, v := ModelTypeAndValue(model)
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		fv := v.Field(i)
		if !def.fieldFilter(ft) || ft.Name == def.key {
			continue
		}
		//TODO:proto timestamp to time.Time
		if ft.Type == ProtoTimestampType {
			t, _ := ptypes.Timestamp(fv.Interface().(*timestamp.Timestamp))
			raw[ft.Name] = t
			continue
		}
		raw[ft.Name] = fv.Interface()
	}
	return nil
}

func (def *Descriptor) UpdateRecord(raw map[string]interface{}) error {
	ff := []string{}
	for k := range raw {
		if k != def.key {
			s := k + "=:" + k
			ff = append(ff, s)
		}
	}
	cmd := "update " + def.table + " set " + strings.Join(ff, ",") + " where " + def.key + "=:" + def.key
	_, err := def.backend.DB.NamedExec(cmd, raw)
	if err != nil {
		return err
	}
	return nil
}

func (def *Descriptor) RemoveRecord(key interface{}) error {
	cmd := fmt.Sprintf("delete from %s where %s=?", def.table, def.key)
	_, err := def.backend.DB.Exec(cmd, key)
	if err != nil {
		return err
	}
	return nil
}