package gocqltable

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/gocql/gocql"

	r "github.com/kristoiv/gocqltable/reflect"
)

type TableInterface interface {
	Create() error
	Drop() error
	Query(statement string, params ...interface{}) Query
	Name() string
	Keyspace() Keyspace
	RowKeys() []string
	RangeKeys() []string
	Model() interface{}
	Rows() []string
}

type Table struct {
	name      string
	rowKeys   []string
	rangeKeys []string
	rows      []string
	model     interface{}

	keyspace Keyspace
	session  *gocql.Session
}

func NewTable(keyspace Keyspace,
	model interface{}) (*Table, error) {

	result := &Table{
		keyspace: keyspace,
		model:    model,
	}

	result.InitName()

	err := result.InitRowKeys()
	if err != nil {
		return nil, err
	}

	err = result.InitRangeKeys()
	if err != nil {
		return nil, err
	}

	err = result.InitRows()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (t Table) Create() error {
	return t.create()
}

func (t Table) CreateWithProperties(props ...string) error {
	return t.create(props...)
}

func (t Table) create(props ...string) error {

	if t.session == nil {
		t.session = defaultSession
	}

	createSchema, err := t.GetCreateSchema()
	if err != nil {
		return err
	}

	return t.session.Query(createSchema).Exec()
}

func (t Table) GetCreateSchema(props ...string) (string, error) {

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	pkString := "PRIMARY KEY ((" + strings.Join(rowKeys, ", ") + ")"
	if len(rangeKeys) > 0 {
		pkString = pkString + ", " + strings.Join(rangeKeys, ", ")
	}
	pkString = pkString + ")"

	fields := []string{}

	m, ok := r.StructToMap(t.Model())
	if !ok {
		return "", fmt.Errorf("Unable to get map from struct during create table")
	}

	keyLen := len(m)
	sortedKeys := make([]string, keyLen)
	i := 0
	for key := range m {
		sortedKeys[i] = key
		i++
	}

	sort.Slice(sortedKeys, func(i, j int) bool {
		return strings.Compare(sortedKeys[i], sortedKeys[j]) < 0
	})

	for _, key := range sortedKeys {
		typ, err := stringTypeOf(m[key])
		if err != nil {
			return "", err
		}
		fields = append(fields, fmt.Sprintf(`%s %v`, key, typ))
	}

	// Add primary key value to fields list
	fields = append(fields, pkString)

	propertiesString := ""
	if len(props) > 0 {
		propertiesString = "WITH " + strings.Join(props, " AND ")
	}

	if len(fields) > 0 {
		fields[0] = fmt.Sprintf("\n%s", fields[0])
	}
	return fmt.Sprintf(`CREATE TABLE %q.%q (%s) %s`,
		t.Keyspace().Name(), t.Name(), strings.Join(fields, ",\n"), propertiesString), nil
}

func (t Table) Drop() error {
	if t.session == nil {
		t.session = defaultSession
	}
	return t.session.Query(fmt.Sprintf(`DROP TABLE %q.%q`, t.Keyspace().Name(), t.Name())).Exec()
}

func (t Table) Query(statement string, values ...interface{}) Query {
	if t.session == nil {
		t.session = defaultSession
	}
	return Query{
		Statement: statement,
		Values:    values,

		Table:   t,
		Session: t.session,
	}
}

func (t Table) Name() string {
	return t.name
}

func (t Table) Keyspace() Keyspace {
	return t.keyspace
}

func (t Table) RowKeys() []string {
	return t.rowKeys
}

func (t Table) RangeKeys() []string {
	return t.rangeKeys
}

func (t Table) Model() interface{} {
	return t.model
}

func (t Table) Rows() []string {
	return t.rows
}

func (t *Table) InitName() {
	t.name = GetTableName(t.model)
}

func (t *Table) InitRowKeys() error {
	rowKeys, err := getNamesWithTag(t.model, "cqlx", "partkey")
	if err != nil {
		return err
	}

	for i, key := range rowKeys {
		rowKeys[i] = t.GetCqlName(key)
	}

	t.rowKeys = rowKeys

	return nil
}

func (t *Table) GetCqlName(attName string) string {
	modelValue := reflect.ValueOf(t.model)
	modelIndirect := reflect.Indirect(modelValue)
	modelType := modelIndirect.Type()
	field, ok := modelType.FieldByName(attName)
	if !ok {
		return ""
	}

	cqlName := field.Tag.Get("cql")
	if len(cqlName) == 0 {
		return ""
	}

	return cqlName
}
func (t *Table) InitRangeKeys() error {
	rangeKeys, err := getNamesWithTag(t.model, "cqlx", "softkey")
	if err != nil {
		return err
	}

	for i, key := range rangeKeys {
		rangeKeys[i] = t.GetCqlName(key)
	}

	t.rangeKeys = rangeKeys

	return nil
}

func (t *Table) InitRows() error {
	rows, err := getNamesWithTag(t.model, "cql", "")
	if err != nil {
		return err
	}

	t.rows = rows
	for i, row := range t.rows {
		t.rows[i] = t.GetCqlName(row)
	}
	sort.Slice(t.rows, func(i, j int) bool {
		return strings.Compare(t.rows[i], t.rows[j]) < 0
	})

	return nil
}

func GetTableName(model interface{}) string {
	modelType := reflect.TypeOf(model)
	m, ok := modelType.MethodByName("TableName")
	if ok {
		returnVals := m.Func.Call(
			[]reflect.Value{reflect.ValueOf(model)})
		return returnVals[0].String()
	}

	modelName := fmt.Sprintf("%T", model)

	result := ToSnakeCase(fmt.Sprintf("%ss",
		strings.Split(modelName, ".")[strings.Count(modelName, ".")]))

	return result
}

func getNamesWithTag(model interface{}, tag, match string) ([]string, error) {
	result := make([]string, 0)
	val := reflect.ValueOf(model)
	valIndirect := reflect.Indirect(val)
	if !valIndirect.IsValid() {
		return nil, fmt.Errorf("model is invalid")
	}
	valType := valIndirect.Type()
	numFields := valType.NumField()

	startMatch := regexp.MustCompile(fmt.Sprintf("%s;", match))
	endMatch := regexp.MustCompile(fmt.Sprintf(";%s", match))
	middleMatch := regexp.MustCompile(fmt.Sprintf(";%s;", match))
	matchRegexs := []*regexp.Regexp{startMatch, middleMatch, endMatch}

	for i := 0; i < numFields; i++ {
		name := valType.Field(i).Name
		value := valType.Field(i).Tag.Get(tag)
		if len(value) == 0 {
			continue
		}
		if len(match) != 0 {
			isMatch := match == value
			if !isMatch {
				for _, matchRegex := range matchRegexs {
					if matchRegex.MatchString(match) {
						isMatch = true
						break
					}
				}
			}

			if !isMatch {
				continue
			}
		}

		result = append(result, name)
	}

	return result, nil
}
