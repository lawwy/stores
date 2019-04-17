package sql4

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/joho/godotenv"
)

type Demo struct {
	Id        string               `json:"id"`
	Name      string               `json:"name"`
	Content   string               `json:"content"`
	I1        int                  `json:"i1"`
	I32       int32                `json:"i32"`
	I64       int64                `json:"i64"`
	F32       float32              `json:"f32"`
	F64       float64              `json:"f64"`
	B         bool                 `json:"b"`
	CreatedAt time.Time            `json:"createdAt"`
	UpdatedAt *timestamp.Timestamp `json:"updatedAt"`
	Others    string               `json:"-"`
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
	if d1.Id == d2.Id && d1.Name == d2.Name && d1.Content == d2.Content && d1.I1 == d2.I1 && d1.I32 == d2.I32 && d1.I64 == d1.I64 && d1.F32 == d2.F32 && d1.F64 == d2.F64 && d1.B == d2.B && (timediff < 50 && timediff > -50) && (protoTimediff < 5 && protoTimediff > -5) {
		return true
	}
	return false
	// return reflect.DeepEqual(d1, d2)
}

func TestStore(t *testing.T) {
	store := setupSql()
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
