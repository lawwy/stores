package sql4

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"encoding/json"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/yourhe/stores/queryer"
)

var (
	ProtoTimestampPtrType = reflect.TypeOf(&timestamp.Timestamp{})
	ProtoTimestampType    = reflect.TypeOf(timestamp.Timestamp{})
	OriginTimeType        = reflect.TypeOf(time.Now())
)

var SEP string = ";"

type Store interface {
	Write(interface{}, interface{}) error
	Read(interface{}, interface{}) error // key, out set value interface{}
	ReadAll(interface{}) error
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
	err = mdef.Record2Model(m, model)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBackend) ReadAll(mlist interface{}) error {
	t, lv := ListTypeAndValue(mlist)
	mdef := s.ModelDesriptor(reflect.New(t).Interface())
	rr, err := mdef.Records(nil)
	if err != nil {
		return err
	}
	for _, r := range rr {
		vptr := reflect.New(t)
		err = mdef.Record2Model(r, vptr.Interface())
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
	err = mdef.Model2Record(model, r)
	if err != nil {
		return err
	}
	return mdef.UpdateRecord(r)
}

// type SearchOption struct {
// 	Limit  int32
// 	Offset int32
// 	Order  string
// 	Query  string
// }

//TODO:重构
func (s *SqlBackend) Find(query interface{}, mlist interface{}) error {
	t, lv := ListTypeAndValue(mlist)
	mdef := s.ModelDesriptor(reflect.New(t).Interface())
	rr, err := mdef.Records(query)
	if err != nil {
		return err
	}
	for _, r := range rr {
		vptr := reflect.New(t)
		err = mdef.Record2Model(r, vptr.Interface())
		if err != nil {
			return nil
		}
		lv.Set(reflect.Append(lv, vptr))
	}
	return nil
}

type Descriptor struct {
	table          string
	key            string
	autoKey        string
	backend        *SqlBackend
	fieldFilter    StructFieldFilter
	fieldMapper    StructFieldMapper
	typeConverters map[reflect.Type]*TypeConverter
}

func (s *SqlBackend) ModelDesriptor(model interface{}) *Descriptor {
	t, _ := ModelTypeAndValue(model)
	return &Descriptor{
		table:          t.Name(),
		key:            "Id",
		backend:        s,
		fieldFilter:    ProtoFieldFilter,
		fieldMapper:    DefaultFieldMapper,
		autoKey:        "autoId",
		typeConverters: s.typeConverters,
	}
}

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

func (def *Descriptor) Records(q interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("select * from %s", def.table)
	if q != nil {
		searchOpt := q.(*SearchOption)
		sql, err := searchOpt.Source(&queryer.Sql{})
		if err != nil {
			return nil, nil
		}
		cond := sql.(string)
		query = query + " " + cond
	}
	fmt.Println(query)
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

func (def *Descriptor) Record2Model(raw map[string]interface{}, model interface{}) error {
	mt, mv := ModelTypeAndValue(model)
	// fields := StructFields(mt, def.fieldFilter)
	for i := 0; i < mt.NumField(); i++ {
		fv := mv.Field(i)
		ft := mt.Field(i)
		v, ok := raw[def.fieldMapper(ft)] //是否需要mapper
		if !ok {
			continue
		}
		if !def.fieldFilter(ft) {
			continue
		}
		if converter, ok := def.typeConverters[ft.Type]; ok {
			converter.DBRecord2Model(fv, v)
		}
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
			// if reflect.TypeOf(v).AssignableTo(fv.Type()) {
			// 	fv.Set(reflect.ValueOf(v))
			// 	// break
			// }
			if ft.Type == OriginTimeType {
				fv.Set(reflect.ValueOf(v))
				break
			}
			//普通结构体情况
			_v := v.([]byte)
			json.Unmarshal(_v, fv.Addr().Interface())
		case reflect.Ptr:
			if ft.Type == ProtoTimestampPtrType {
				_time, _ := reflect.ValueOf(v).Interface().(time.Time)
				_v, _ := ptypes.TimestampProto(_time)
				fv.Set(reflect.ValueOf(_v))
				break
			}
			if ft.Type.Elem().Kind() == reflect.Struct {
				_v := v.([]byte)
				json.Unmarshal(_v, fv.Addr().Interface())
				// fv.Set(reflect.ValueOf())
			}
		case reflect.Slice:
			subType := ft.Type.Elem()

			if subType.Kind() == reflect.String {
				_v := v.([]uint8)
				_s := B2S(_v)
				fv.Set(reflect.ValueOf(strings.Split(_s, SEP)))
			}
			if subType.Kind() == reflect.Ptr || subType.Kind() == reflect.Struct {
				_v := v.([]byte)
				json.Unmarshal(_v, fv.Addr().Interface())
			}
		}
	}
	return nil
}

func (def *Descriptor) Model2Record(model interface{}, raw map[string]interface{}) error {
	t, v := ModelTypeAndValue(model)
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		fv := v.Field(i)
		if !def.fieldFilter(ft) || ft.Name == def.key {
			continue
		}
		dbfname := def.fieldMapper(ft)
		if dbfname == "" {
			continue
		}
		if converter, ok := def.typeConverters[ft.Type]; ok {
			raw[dbfname] = converter.Model2DBRecord(fv)
			continue
		}
		switch fv.Kind() {
		case reflect.Ptr:
			if ft.Type == ProtoTimestampPtrType {
				t, _ := ptypes.Timestamp(fv.Interface().(*timestamp.Timestamp))
				raw[dbfname] = t
				break
			}
			if ft.Type.Elem().Kind() == reflect.Struct {
				b, _ := json.Marshal(fv.Interface())
				raw[dbfname] = string(b)
				break
			}
		case reflect.Struct:
			if ft.Type == OriginTimeType {
				raw[dbfname] = fv.Interface()
				break
			}
			b, _ := json.Marshal(fv.Interface())
			raw[dbfname] = string(b)
		case reflect.Slice:
			subType := ft.Type.Elem()
			if subType.Kind() == reflect.String {
				ss, _ := fv.Interface().([]string)
				raw[dbfname] = strings.Join(ss, SEP)
				break
			}
			if subType.Kind() == reflect.Ptr || subType.Kind() == reflect.Struct {
				b, _ := json.Marshal(fv.Interface())
				raw[dbfname] = string(b)
				break
			}
		default:
			raw[dbfname] = fv.Interface()
		}
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
