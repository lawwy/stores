package sql4

import (
	"reflect"
	"strings"

	// "database/sql"
	sql "github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

// type Backend interface {
// 	Insert(interface{}) error
// 	Get(interface{}, interface{}) error
// 	Delete(interface{}) error
// 	List(interface{}) error
// 	Update(interface{}) error
// 	Paginate(interface{}, int, int) error
// }

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
	DB             *DB
	autoKey        string
	table          string
	pk             string
	fieldFilter    StructFieldFilter
	fieldMapper    StructFieldMapper
	typeConverters map[reflect.Type]*TypeConverter
}

type TypeConverter struct {
	DBRecord2Model     func(reflect.Value, interface{})
	Model2DBRecord     func(reflect.Value) interface{}
	Model2DBDefinition func() string
}

func NewSqlBackend(db *DB) (*SqlBackend, error) {
	sql := &SqlBackend{
		DB:             db,
		typeConverters: make(map[reflect.Type]*TypeConverter),
	}
	// sql.fieldMapper = DefaultFieldMapper
	// sql.SetFieldFilter(DefaultFieldFilter)
	return sql, nil
}

func (s *SqlBackend) RegisterTypeConverter(t reflect.Type, convertor *TypeConverter) {
	s.typeConverters[t] = convertor
}

// func (s *SqlBackend) SetFieldFilter(fn StructFieldFilter) {
// 	s.fieldFilter = fn
// }

// func (s *SqlBackend) SetFieldMapper(fn StructFieldMapper) {
// 	s.fieldMapper = fn
// }

// func (s *SqlBackend) SetPKField(name string) {
// 	if s.pk == "" {
// 		s.pk = name
// 	}
// }
