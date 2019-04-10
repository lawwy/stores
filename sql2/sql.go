package sql2

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	// "github.com/jmoiron/sqlx"
)

type Backend interface {
	Insert(interface{}) (int64, error)
	Get(int64, interface{}) error
	Delete(int64, interface{}) error
	List(interface{}) error
	Update(int64, interface{}) error
	Paginate(interface{}, int, int) error
}

type Builder interface {
	Migrate(interface{}) error
}

type DB struct {
	*sql.DB
}

func NewDB(sqlType string, conn string) (*DB, error) {
	db, err := sql.Open(sqlType, conn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

type StructFieldFilter func(reflect.StructField) bool
type StructFieldMapper func(reflect.StructField) string

var (
	DefaultFieldFilter = func(reflect.StructField) bool { return true }
	DefaultFieldMapper = func(f reflect.StructField) string { return f.Name }
	LowerFieldMapper   = func(f reflect.StructField) string { return strings.ToLower(f.Name) }
	ProtoFieldFilter   = func(f reflect.StructField) bool {
		if f.Tag.Get("json") != "-" {
			return true
		}
		return false
	}
)

type SqlBackend struct {
	DB          *DB
	table       string
	pk          string
	fieldFilter StructFieldFilter
	fieldMapper StructFieldMapper
}

func NewSqlBackend(db *DB) (*SqlBackend, error) {
	sql := &SqlBackend{
		DB: db,
	}
	sql.SetFieldMapper(DefaultFieldMapper)
	sql.SetFieldFilter(DefaultFieldFilter)
	return sql, nil
}

func (s *SqlBackend) SetFieldFilter(fn StructFieldFilter) {
	s.fieldFilter = fn
}

func (s *SqlBackend) SetFieldMapper(fn StructFieldMapper) {
	s.fieldMapper = fn
}

func (s *SqlBackend) SetPKField(name string) {
	if s.pk == "" {
		s.pk = name
	}
}

func (s *SqlBackend) SetTable(name string) {
	if s.table == "" {
		s.table = name
	}
}

func (s *SqlBackend) Migrate(model interface{}) error {
	//TODO:
	//若表不存在，则创建表
	//若表存在修改，则修改表(预计只能增加字段)
	if exist, _ := s.Exist(model); !exist {
		err := s.Create(model)
		return err
	}
	err := s.Alter(model)
	if err != nil {
		return nil
	}
	return nil
}

func (s *SqlBackend) Drop(model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	table := s.TableName(t.Name())
	drop := fmt.Sprintf("DROP TABLE %s;", table)
	_, err := s.DB.Exec(drop)
	return err
}

func (s *SqlBackend) Exist(model interface{}) (bool, error) {
	t := reflect.TypeOf(model).Elem()
	table := s.TableName(t.Name())
	exist := false
	q := fmt.Sprintf("SELECT table_name FROM information_schema.TABLES WHERE table_name ='%s';", table)
	rows, err := s.DB.Query(q)
	if err != nil {
		return exist, err
	}
	defer rows.Close()
	for rows.Next() {
		exist = true
		break
	}
	return exist, err
}

func (s *SqlBackend) Create(model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	ff := StructFields(t, s.Filter("CreateTable"))
	sql := s.Sql("CreateTable", s.TableName(t.Name()), ff)
	_, err := s.DB.Exec(sql)
	return err
}

//暂时只增加不存在的字段
func (s *SqlBackend) Alter(model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	table := s.TableName(t.Name())
	cols, err := s.getTableColumn(table)
	if err != nil {
		return err
	}
	ff := StructFields(t, func(f reflect.StructField) bool {
		name := s.fieldMapper(f)
		notExist := true
		for _, c := range cols {
			if c == name {
				notExist = false
			}
		}
		return s.fieldFilter(f) && notExist
	})
	if len(ff) == 0 {
		return nil
	}
	sql := s.Sql("AlterTable", table, ff)
	_, err = s.DB.Exec(sql)
	return err
}

func (s *SqlBackend) getTableColumn(table string) ([]string, error) {
	colsql := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'", table)
	rows, err := s.DB.Query(colsql)
	if err != nil {
		return nil, nil
	}
	defer rows.Close()
	cols := []string{}
	for rows.Next() {
		var col string
		err = rows.Scan(&col)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func (s *SqlBackend) Insert(model interface{}) (int64, error) {
	t := reflect.TypeOf(model).Elem()
	v := reflect.Indirect(reflect.ValueOf(model))
	ff := StructFields(t, s.Filter("Insert"))
	vv := FieldsValues(v, ff)
	query := s.Sql("Insert", s.TableName(t.Name()), ff)
	res, err := s.DB.Exec(query, vv...)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *SqlBackend) Get(key int64, model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	v := reflect.Indirect(reflect.ValueOf(model))
	ff := StructFields(t, s.Filter("Get"))
	ptrs := FieldsPointers(v, ff)
	query := s.Sql("Get", s.TableName(t.Name()), ff) + " where " + s.IdCond(key)
	err := s.DB.QueryRow(query).Scan(ptrs...)
	return err
}

func (s *SqlBackend) Update(key int64, model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	v := reflect.Indirect(reflect.ValueOf(model))
	ff := StructFields(t, s.Filter("Update"))
	vv := FieldsValues(v, ff)
	query := s.Sql("Update", s.TableName(t.Name()), ff) + " where " + s.IdCond(key)
	_, err := s.DB.Exec(query, vv...)
	return err
}

func (s *SqlBackend) IdCond(id int64) string {
	return s.pk + "=" + strconv.FormatInt(id, 10)
}

func (s *SqlBackend) List(list interface{}) error {
	lv := reflect.ValueOf(list).Elem()             //切片值(非指针)
	t := reflect.TypeOf(list).Elem().Elem().Elem() //切片成员类型(非指针)
	ff := StructFields(t, s.Filter("List"))
	query := s.Sql("List", s.TableName(t.Name()), ff)
	rows, err := s.DB.Query(query)
	defer rows.Close()
	for rows.Next() {
		vptr := reflect.New(t)
		ptrs := FieldsPointers(reflect.Indirect(vptr), ff)
		err := rows.Scan(ptrs...)
		if err != nil {
			return err
		}
		lv.Set(reflect.Append(lv, vptr))
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBackend) Paginate(list interface{}, offset int, limit int) error {
	lv := reflect.ValueOf(list).Elem()             //切片值(非指针)
	t := reflect.TypeOf(list).Elem().Elem().Elem() //切片成员类型(非指针)
	ff := StructFields(t, s.Filter("List"))
	query := s.Sql("List", s.TableName(t.Name()), ff) + " " + s.pageCond(offset, limit)
	fmt.Println("Paginate:", query)
	rows, err := s.DB.Query(query)
	defer rows.Close()
	for rows.Next() {
		vptr := reflect.New(t)
		ptrs := FieldsPointers(reflect.Indirect(vptr), ff)
		err := rows.Scan(ptrs...)
		if err != nil {
			return err
		}
		lv.Set(reflect.Append(lv, vptr))
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBackend) pageCond(offset int, limit int) string {
	// if s.pk != "" {
	// 	return fmt.Sprintf("%s > %s limit %s", s.pk, offset, limit)
	// }
	return fmt.Sprintf("limit %d,%d", offset, limit)
}

func (s *SqlBackend) Delete(key int64, model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	query := s.Sql("Delete", s.TableName(t.Name()), nil) + " where " + s.IdCond(key)
	_, err := s.DB.Exec(query)
	return err
}

func (s *SqlBackend) TableFieldsNames(fields []reflect.StructField) []string {
	fnames := make([]string, len(fields))
	for i, f := range fields {
		fnames[i] = s.fieldMapper(f)
	}
	return fnames
}

// CREATE TABLE demo2 (
// 	ID INT AUTO_INCREMENT PRIMARY KEY,
// 	Name VARCHAR(100) NULL,
// 	Content TEXT NULL,
// 	CreatedAt DATETIME NULL
// )
// `

func (s *SqlBackend) TableFieldDefinition(field reflect.StructField) string {
	name := s.fieldMapper(field)
	def := ""
	switch field.Type.Kind() {
	case reflect.Bool:
		def = "BOOLEAN DEFAULT false"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr, reflect.Int64, reflect.Uint64:
		if name == s.pk {
			def = "INT AUTO_INCREMENT PRIMARY KEY"
		} else {
			def = "INT DEFAULT 0"
		}
	case reflect.Float32, reflect.Float64:
		def = "DOUBLE DEFAULT 0"
	case reflect.String:
		def = "VARCHAR(255) DEFAULT ''"
		if name == s.pk {
			def = "VARCHAR(255) PRIMARY KEY"
		}
	case reflect.Struct:
		//QUESTIONS:如何满足各种struct类型的要求？proto中的timestamp如何处理(是否需要这样的兼容)？
		//如兼容需要在db读写过程进行字段值的转换，转换方法如何定？
		if field.Type.PkgPath() == "time" {
			def = "DATETIME DEFAULT CURRENT_TIMESTAMP"
		}
	}
	return name + " " + def
}

func (s *SqlBackend) TableName(modelName string) string {
	if s.table == "" {
		return modelName
	}
	return s.table
}

func (s *SqlBackend) Sql(t string, table string, fields []reflect.StructField) string {
	switch t {
	case "List", "Get":
		fnames := s.TableFieldsNames(fields)
		return "SELECT " + strings.Join(fnames, ",") + " FROM " + table

	case "Insert":
		fnames := s.TableFieldsNames(fields)
		stubs := MakeStubs("?", len(fields))
		fs := "(" + strings.Join(fnames, ",") + ")"
		vs := "(" + strings.Join(stubs, ",") + ")"
		return "INSERT INTO " + table + " " + fs + " VALUES " + vs

	case "Update":
		fnames := s.TableFieldsNames(fields)
		stubs := MapStrings(fnames, func(f string) string {
			return f + "=?"
		})
		return "UPDATE " + table + " SET " + strings.Join(stubs, ",")

	case "Delete":
		return "DELETE FROM " + table
	case "CreateTable":
		typeDefs := make([]string, len(fields))
		for i := range typeDefs {
			typeDefs[i] = s.TableFieldDefinition(fields[i])
		}
		return "CREATE TABLE " + table + " (" + strings.Join(typeDefs, ",") + ")"
	case "AlterTable":
		alters := make([]string, len(fields))
		for i := range alters {
			alters[i] = "ADD COLUMN " + s.TableFieldDefinition(fields[i])
		}
		return "ALTER TABLE " + table + " " + strings.Join(alters, ",") + ";"
	}
	return ""
}

func (s *SqlBackend) Filter(t string) StructFieldFilter {
	//t : List Get Insert Update
	switch t {
	case "List", "Get", "CreateTable":
		return s.fieldFilter
	case "Insert", "Update":
		return func(f reflect.StructField) bool {
			isPk := false
			if f.Name == s.pk {
				isPk = true
			}
			return s.fieldFilter(f) && !isPk
		}
	}
	return s.fieldFilter
}

func StructFields(t reflect.Type, need StructFieldFilter) []reflect.StructField {
	ff := []reflect.StructField{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if need(f) {
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
