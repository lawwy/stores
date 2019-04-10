package sql2

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

type Demo1 struct {
	ID      int64
	Name    string
	Content string
}

const CREATE_DEMO1_SQL = `
	CREATE TABLE demo1 (
		id INT NOT NULL AUTO_INCREMENT,
		name VARCHAR(100) NULL,
		content TEXT NULL,
		PRIMARY KEY (id));
`
const DROP_DEMO1_SQL = `DROP TABLE demo1;`

type Demo2 struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"create_at"`
	XXX       string    `json:"-"`
}

const CREATE_DEMO2_SQL = `
CREATE TABLE demo2 (
	ID INT AUTO_INCREMENT PRIMARY KEY,
	Name VARCHAR(100) NULL,
	Content TEXT NULL,
	CreatedAt DATETIME NULL
)
`
const DROP_DEMO2_SQL = `DROP TABLE Demo2;`

func Test_Curd(t *testing.T) {
	check := CheckErrFunc(t)
	err := godotenv.Load("../.env")
	check(err)
	db, _ := NewDB(os.Getenv("SQL_TYPE"), os.Getenv("SQL_CONNECTION"))
	// _, err = db.Exec(CREATE_DEMO1_SQL)
	// check(err)
	// defer db.Exec(DROP_DEMO1_SQL)

	sql, _ := NewSqlBackend(db)
	sql.SetFieldMapper(LowerFieldMapper)
	sql.SetPKField("id")
	sql.SetTable("demo1")
	err = sql.Migrate(&Demo1{})
	check(err)
	defer sql.Drop(&Demo1{})
	dd := []*Demo1{}
	err = sql.List(&dd)
	check(err)
	if len(dd) != 0 {
		t.Fatal("empty get fail")
	}
	r := &Demo1{
		Name:    "n1",
		Content: "n1",
	}
	id, _ := sql.Insert(r)
	if id != 1 {
		t.Fatal("create fail")
	}
	rr := []*Demo1{}
	_ = sql.List(&rr)
	// check(err)
	if len(rr) <= 0 {
		t.Fatal("list fail")
	}
	nr := &Demo1{}
	_ = sql.Get(1, nr)
	if nr.ID != 1 || nr.Name != "n1" || nr.Content != "n1" {
		t.Fatal("get fail")
	}
	nr.Content = "new"
	nr.Name = "new"
	_ = sql.Update(1, nr)
	nnr := &Demo1{}
	_ = sql.Get(1, nnr)
	if nnr.ID != 1 || nnr.Name != "new" || nnr.Content != "new" {
		t.Fatal("update fail")
	}
	var rp *Demo1
	err = sql.Delete(1, rp)
	check(err)
	nrr := []*Demo1{}
	_ = sql.List(&nrr)
	if len(nrr) != 0 {
		t.Fatal("delete fail")
	}
}

func Test_Paginate(t *testing.T) {
	check := CheckErrFunc(t)
	err := godotenv.Load("../.env")
	check(err)
	db, _ := NewDB(os.Getenv("SQL_TYPE"), os.Getenv("SQL_CONNECTION"))
	sql, _ := NewSqlBackend(db)
	sql.SetFieldMapper(LowerFieldMapper)
	sql.SetPKField("id")
	sql.SetTable("demo1")
	err = sql.Migrate(&Demo1{})
	defer sql.Drop(&Demo1{})
	ndd := []*Demo1{
		&Demo1{
			Name:    "name1",
			Content: "content1",
		},
		&Demo1{
			Name:    "name2",
			Content: "content2",
		},
		&Demo1{
			Name:    "name3",
			Content: "content3",
		},
		&Demo1{
			Name:    "name4",
			Content: "content4",
		},
	}
	for _, d := range ndd {
		_, err = sql.Insert(d)
		check(err)
	}
	dd := []*Demo1{}
	err = sql.Paginate(&dd, 1, 2)
	check(err)
	// fmt.Println(dd[0])
	want := []*Demo1{
		&Demo1{
			ID:      2,
			Name:    "name2",
			Content: "content2",
		},
		&Demo1{
			ID:      3,
			Name:    "name3",
			Content: "content3",
		},
	}
	if !reflect.DeepEqual(&dd, &want) {
		t.Fatal("paginate fail")
	}
}

func Test_withProtoFilter(t *testing.T) {
	check := CheckErrFunc(t)
	err := godotenv.Load("../.env")
	check(err)
	db, _ := NewDB(os.Getenv("SQL_TYPE"), os.Getenv("SQL_CONNECTION"))
	// _, err = db.Exec(CREATE_DEMO2_SQL)
	// check(err)
	// defer db.Exec(DROP_DEMO2_SQL)

	sql, _ := NewSqlBackend(db)
	sql.SetPKField("ID")
	sql.SetFieldFilter(ProtoFieldFilter)
	err = sql.Migrate(&Demo2{})
	check(err)
	defer sql.Drop(&Demo2{})
	r := &Demo2{
		Name:      "n1",
		Content:   "n1",
		CreatedAt: time.Now(),
	}
	id, err := sql.Insert(r)
	check(err)
	if id != 1 {
		t.Fatal("create fail")
	}
	rr := []*Demo2{}
	err = sql.List(&rr)
	check(err)
	if len(rr) <= 0 {
		t.Fatal("list fail")
	}
	nr := &Demo2{}
	err = sql.Get(1, nr)
	check(err)
	if nr.ID != 1 || nr.Name != "n1" || nr.Content != "n1" {
		t.Fatal("get fail")
	}
	nr.Content = "new"
	nr.Name = "new"
	nr.CreatedAt = time.Now()
	err = sql.Update(1, nr)
	check(err)
	nnr := &Demo2{}
	err = sql.Get(1, nnr)
	check(err)
	if nnr.ID != 1 || nnr.Name != "new" || nnr.Content != "new" {
		t.Fatal("update fail")
	}
	var rp *Demo2
	err = sql.Delete(1, rp)
	check(err)
	nrr := []*Demo2{}
	err = sql.List(&nrr)
	check(err)
	if len(nrr) != 0 {
		t.Fatal("delete fail")
	}
}

type Demo3 struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"create_at"`
	XXX       string    `json:"-"`
}

const CREATE_DEMO3_SQL = `
CREATE TABLE demo3 (
	ID INT AUTO_INCREMENT PRIMARY KEY,
	Name VARCHAR(100) NULL,
	Content TEXT NULL
)`

func Test_Migration(t *testing.T) {
	check := CheckErrFunc(t)
	err := godotenv.Load("../.env")
	check(err)
	db, _ := NewDB(os.Getenv("SQL_TYPE"), os.Getenv("SQL_CONNECTION"))
	sql, _ := NewSqlBackend(db)
	sql.SetPKField("ID")
	sql.SetFieldFilter(ProtoFieldFilter)
	exist, err := sql.Exist(&Demo3{})
	check(err)
	if exist == true {
		t.Fatal("exist fail")
	}
	err = sql.Migrate(&Demo3{})
	check(err)
	exist, err = sql.Exist(&Demo3{})
	if exist == false {
		t.Fatal("migrate fail")
	}
	err = sql.Drop(&Demo3{})
	check(err)
	sql.DB.Exec(CREATE_DEMO3_SQL)
	defer sql.Drop(&Demo3{})
	err = sql.Migrate(&Demo3{})
	check(err)
	_, err = sql.Insert(&Demo3{
		Name:      "n1",
		Content:   "n1",
		CreatedAt: time.Now(),
	})
	check(err)

}

func CheckErrFunc(t *testing.T) func(error) {
	return func(e error) {
		if e != nil {
			t.Fatal(e)
		}
	}
}
