package queryer

import (
	"reflect"
	"testing"
)

func Test_QueryBool2(t *testing.T) {
	id := NewTermQuery("id", 1)
	name := NewTermQuery("name", "pk")
	query1 := NewBoolQuery().Should(name).Must(id)

	query := NewBoolQuery().Must(id).Must(name).Should(query1).Should(name).Should(query1)
	t.Error(query)

	t.Error(query.Source(&Sql{}))
	t.Error(query1.Source(&Sql{}))
}

func Test_QueryBool(t *testing.T) {
}

// term := queryer.NewTermQuery("role_id", req.Id)
// query := queryer.NewBoolQuery()
// query.Must(term)
// q := query.Source(s.store.)

// q := fmt.Sprintf("role_id Like '%s'",req.Id)

// err = s.find(ctx, q , &result)

func TestNewBoolQuery(t *testing.T) {
	tests := []struct {
		name  string
		query *BoolQuery
		want  string
	}{
		// TODO: Add test cases.
		{
			name:  "must",
			query: NewBoolQuery().Must(NewTermQuery("id", 1)),
			want:  "id = 1",
		},
		{
			name:  "should",
			query: NewBoolQuery().Should(NewTermQuery("id", 1)),
			want:  "id = 1",
		},
		{
			name:  "crose",
			query: NewBoolQuery().Must(NewTermQuery("id", 1)).Should(NewTermQuery("name", "abc")),
			want:  "(id = 1 or name = 'abc')",
		},
		{
			name: "crose",
			query: NewBoolQuery().
				Must(NewTermQuery("must", "must")).
				Must((NewBoolQuery().Must(NewTermQuery("id", 1)).Should(NewTermQuery("name", "abc")))),
			want: "(must = 'must' and (id = 1 or name = 'abc'))",
		},
		{
			name: "like",
			query: NewBoolQuery().
				Must(NewMatchQuery("must", "us")),
			want: "must like '%us%'",
		},
		{
			name: "match operator or",
			query: NewBoolQuery().
				Must(NewMatchQuery("must", "us as")),
			want: "(must like '%us%' or must like '%as%')",
		},
		{
			name: "match operator and",
			query: NewBoolQuery().
				Must(NewMatchQuery("must", "us as").Operator("and")),
			want: "(must like '%us%' and must like '%as%')",
		},
		{
			name: "match operator crose",
			query: NewBoolQuery().
				Should(NewTermQuery("should", "or")).
				Must(NewMatchQuery("must", "us as").Operator("and")),
			want: "((must like '%us%' and must like '%as%') or should = 'or')",
		},
		{
			name: "match operator crose",
			query: NewBoolQuery().
				Must(NewTermQuery("should", "and")).
				Must(NewMatchQuery("must", "us as").Operator("and")),
			want: "(should = 'and' and (must like '%us%' and must like '%as%'))",
		},
		{
			name: "match operator crose",
			query: NewBoolQuery().
				Should(NewMatchQuery("name", "name")).
				Should(NewMatchQuery("nick_name", "nick_name")),
			want: "(name like '%name%' or nick_name like '%nick_name%')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.query.Source(&Sql{})
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBoolQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
