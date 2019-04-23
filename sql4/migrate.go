package sql4

import (
	"fmt"
	"reflect"
	"strings"
)

type Builder interface {
	Migrate(interface{}) error
}

func (s *SqlBackend) Migrate(model interface{}) error {
	def := s.ModelDesriptor(model)
	return def.Migrate(model)
}

func (s *SqlBackend) Drop(model interface{}) error {
	def := s.ModelDesriptor(model)
	return def.Drop()
}

func (def *Descriptor) Migrate(model interface{}) error {
	if exist, _ := def.Exist(model); !exist {
		err := def.Create(model)
		return err
	}
	err := def.Alter(model)
	if err != nil {
		return nil
	}
	return nil
}

func (def *Descriptor) Drop() error {
	drop := fmt.Sprintf("DROP TABLE %s;", def.table)
	_, err := def.backend.DB.Exec(drop)
	return err
}

func (def *Descriptor) Exist(model interface{}) (bool, error) {
	exist := false
	q := fmt.Sprintf("SELECT table_name FROM information_schema.TABLES WHERE table_name ='%s';", def.table)
	rows, err := def.backend.DB.Query(q)
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

func (def *Descriptor) Create(model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	ff := StructFields(t, def.fieldFilter)
	typeDefs := []string{}
	if def.autoKey != "" {
		typeDefs = append(typeDefs, def.AutoKeyDefinition())
	}
	for _, f := range ff {
		typeDefs = append(typeDefs, def.TableFieldDefinition(f))
	}
	cmd := "CREATE TABLE " + def.table + " (" + strings.Join(typeDefs, ",") + ")"
	_, err := def.backend.DB.Exec(cmd)
	return err
}

//暂时只增加不存在的字段
func (def *Descriptor) Alter(model interface{}) error {
	t := reflect.TypeOf(model).Elem()
	cols, err := def.getTableColumn()
	if err != nil {
		return err
	}
	ff := StructFields(t, func(f reflect.StructField) bool {
		notExist := true
		for _, c := range cols {
			if c == f.Name {
				notExist = false
			}
		}
		return def.fieldFilter(f) && notExist
	})
	if len(ff) == 0 {
		return nil
	}

	alters := []string{}
	for _, f := range ff {
		state := "ADD COLUMN " + def.TableFieldDefinition(f)
		alters = append(alters, state)
	}
	cmd := "ALTER TABLE " + def.table + " " + strings.Join(alters, ",") + ";"
	_, err = def.backend.DB.Exec(cmd)
	return err
}

func (def *Descriptor) getTableColumn() ([]string, error) {
	cmd := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'", def.table)
	rows, err := def.backend.DB.Query(cmd)
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

func (def *Descriptor) AutoKeyDefinition() string {
	return def.autoKey + " INT AUTO_INCREMENT PRIMARY KEY NOT NULL"
}

func (def *Descriptor) TableFieldDefinition(field reflect.StructField) string {
	// name := s.fieldMapper(field)
	state := ""
	// fmt.Println(field.Name, field.Type.Kind())
	if converter, ok := def.typeConverters[field.Type]; ok {
		return field.Name + " " + converter.Model2DBDefinition()
		// continue
	}
	switch field.Type.Kind() {
	case reflect.Bool:
		state = "BOOLEAN DEFAULT false"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr, reflect.Int64, reflect.Uint64:
		state = "INT DEFAULT 0"
	case reflect.Float32, reflect.Float64:
		state = "DOUBLE DEFAULT 0"
	case reflect.String:
		state = "VARCHAR(255) DEFAULT ''"
	case reflect.Struct:
		if field.Type == OriginTimeType || field.Type == ProtoTimestampType {
			state = "DATETIME DEFAULT CURRENT_TIMESTAMP"
			break
		} else {
			state = "VARCHAR(255) DEFAULT ''"
		}
	case reflect.Ptr:
		if field.Type == ProtoTimestampPtrType {
			state = "DATETIME DEFAULT CURRENT_TIMESTAMP"
			break
		}
		if field.Type.Elem().Kind() == reflect.Struct {
			state = "VARCHAR(255) DEFAULT ''"
			break
		}
	case reflect.Slice:
		subType := field.Type.Elem()
		if subType.Kind() == reflect.String {
			state = "VARCHAR(255) DEFAULT ''"
		}
		if subType.Kind() == reflect.Struct || subType.Kind() == reflect.Ptr {
			state = "VARCHAR(255) DEFAULT ''"
		}
	}
	return field.Name + " " + state
}
