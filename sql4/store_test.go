package sql4

import (
	"database/sql"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/joho/godotenv"
)

type Demo struct {
	Id          string               `json:"id"`
	Name        string               `json:"name"`
	Content     string               `json:"content"`
	I1          int                  `json:"i1"`
	I32         int32                `json:"i32"`
	I64         int64                `json:"i64"`
	F32         float32              `json:"f32"`
	F64         float64              `json:"f64"`
	B           bool                 `json:"b"`
	CreatedAt   time.Time            `json:"createdAt"`
	UpdatedAt   *timestamp.Timestamp `json:"updatedAt"`
	Keywords    []string             `json:"keywords"`
	Nested1     *NestedDemo          `json:"nested1"`
	Nested2     NestedDemo           `json:"nested2"`
	NestedList1 []*NestedDemo        `json:"nestedlist1"`
	NestedList2 []NestedDemo         `json:"nestedlist2"`
	Others      string               `json:"-"`
	Custom      *Custom              `json:"custom"`
}

type NestedDemo struct {
	Title string `json:"title"`
	Name  string `json:"name"`
}

type Custom struct {
	Offset int
	Limit  int
}

var CustomConverter *TypeConverter = &TypeConverter{
	DBRecord2Model: func(fv reflect.Value, v interface{}) {
		_v := v.([]uint8)
		ss := strings.Split(B2S(_v), ",")
		offset, _ := strconv.Atoi(ss[0])
		limit, _ := strconv.Atoi(ss[1])
		fv.Set(reflect.ValueOf(&Custom{offset, limit}))
	},
	Model2DBRecord: func(v reflect.Value) interface{} {
		offset := v.Elem().Field(0).Interface().(int)
		limit := v.Elem().Field(1).Interface().(int)
		return strconv.Itoa(offset) + "," + strconv.Itoa(limit)
	},
	Model2DBDefinition: func() string {
		return "VARCHAR(45) DEFAULT ''"
	},
}

func setupSql() *SqlBackend {
	err := godotenv.Load("../.env")
	if err != nil {
		panic(err)
	}
	db, _ := NewDB(os.Getenv("SQL_TYPE"), os.Getenv("SQL_CONNECTION"))
	sql, _ := NewSqlBackend(db)
	return sql
}

func equalDemo(d1 *Demo, d2 *Demo) bool {
	timediff := d1.CreatedAt.Unix() - d2.CreatedAt.Unix() //QUESTION:时间有一点偏差
	protoTimediff := d1.UpdatedAt.GetSeconds() - d2.UpdatedAt.GetSeconds()
	isCreatedAtSame := (timediff < 50 && timediff > -50)
	isUpdatedAtSame := (protoTimediff < 5 && protoTimediff > -5)
	isKeywordSame := reflect.DeepEqual(d1.Keywords, d2.Keywords)
	isNested1Same := reflect.DeepEqual(d1.Nested1, d2.Nested1)
	isNested2Same := reflect.DeepEqual(d1.Nested2, d2.Nested2)
	isNestedList1Same := reflect.DeepEqual(d1.NestedList1, d2.NestedList1)
	isNestedList2Same := reflect.DeepEqual(d1.NestedList2, d2.NestedList2)
	isCustomSame := reflect.DeepEqual(d1.Custom, d2.Custom)

	if d1.Id == d2.Id && d1.Name == d2.Name && d1.Content == d2.Content && d1.I1 == d2.I1 && d1.I32 == d2.I32 && d1.I64 == d1.I64 && d1.F32 == d2.F32 && d1.F64 == d2.F64 && d1.B == d2.B && isCreatedAtSame && isUpdatedAtSame && isKeywordSame && isNested1Same && isNested2Same && isNestedList1Same && isNestedList2Same && isCustomSame {
		return true
	}
	return false
	// return reflect.DeepEqual(d1, d2)
}

func TestStore(t *testing.T) {
	store := setupSql()
	store.RegisterTypeConverter(reflect.TypeOf(&Custom{}), CustomConverter)
	err := store.Migrate(&Demo{})
	if err != nil {
		t.Fatal("migrate fail", err)
	}
	// defer store.DB.Exec("TRUNCATE Demo;")
	defer store.Drop(&Demo{})
	demo := &Demo{
		Id:        "id1",
		Name:      "name1",
		Content:   "content1",
		I1:        1,
		I32:       4,
		I64:       5,
		F32:       1.1,
		F64:       1.2,
		B:         true,
		CreatedAt: time.Now(),
		UpdatedAt: ptypes.TimestampNow(),
		Others:    "others1",
		Keywords:  []string{"hello", "world"},
		Nested1:   &NestedDemo{"title1", "name1"},
		Nested2:   NestedDemo{"title2", "name2"},
		NestedList1: []*NestedDemo{
			&NestedDemo{"title3", "name3"},
			&NestedDemo{"title4", "name4"},
		},
		NestedList2: []NestedDemo{
			NestedDemo{"title5", "name5"},
			NestedDemo{"title6", "name6"},
		},
		Custom: &Custom{10, 10},
	}
	err = store.Write(demo.Id, demo)
	if err != nil {
		t.Fatal("write fail", err)
	}

	demo2 := &Demo{}
	err = store.Read(demo.Id, demo2)
	if err != nil || !equalDemo(demo, demo2) {
		t.Fatal("read fail", err)
	}

	demo.Name = "name2"
	err = store.Write(demo.Id, demo)
	_ = store.Read(demo.Id, demo2)
	if err != nil || !equalDemo(demo, demo2) {
		t.Fatal("update fail", err)
	}

	demos := []*Demo{}
	err = store.ReadAll(&demos)
	if err != nil || len(demos) != 1 {
		t.Fatal("readAll fail", err)
	}

	var empty *Demo
	err = store.Write(demo.Id, empty)
	notExistErr := store.Read(demo.Id, &Demo{})
	if err != nil || notExistErr != sql.ErrNoRows {
		t.Fatal("delete fail", err, notExistErr)
	}
}
