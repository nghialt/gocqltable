package gocqltable

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type User struct {
	Email    string    `cql:"email"`
	Password string    `cql:"password"`
	Active   bool      `cql:"active" cqlx:"partkey"`
	Birthday string    `cql:"birthday" cqlx:"partkey"`
	Address  string    `cql:"address" cqlx:"softkey"`
	Sex      int       `cql:"Sex" cqlx:"softkey"`
	Age      uint32    `cql:"Age"`
	Created  time.Time `cql:"created"`
}

type CustomUser struct {
	Email    string `cql:"email"`
	Password string `cql:"password"`
	Birthday string `cql:"birthday" cqlx:"partkey"`
}

func (r CustomUser) TableName() string {
	return "custom_name_abc"
}

func TestGetCreateSchema(t *testing.T) {

	expectSchema := "CREATE TABLE \"keyspace\".\"users\" (\nAge int,\nSex int,\nactive boolean,\naddress varchar,\nbirthday varchar,\ncreated timestamp,\nemail varchar,\npassword varchar,\nPRIMARY KEY ((active, birthday), address, Sex))"

	userTable, err := NewTable(Keyspace{"keyspace", nil}, User{})
	assert.NoError(t, err)

	schema, err := userTable.GetCreateSchema()

	assert.NoError(t, err)
	assert.Equal(t, expectSchema, strings.TrimSpace(schema))
}

func TestInitRowKeys(t *testing.T) {

	expectRowKeys := []string{"active", "birthday"}

	userTable, err := NewTable(Keyspace{"keyspace", nil}, User{})
	assert.NoError(t, err)

	rowKeys := userTable.RowKeys()

	sort.Slice(rowKeys, func(i, j int) bool {
		return strings.Compare(rowKeys[i], rowKeys[j]) < 0
	})
	assert.Equal(t, expectRowKeys, rowKeys)

	expectRangeKeys := []string{"Sex", "address"}

	rangeKeys := userTable.RangeKeys()

	sort.Slice(rangeKeys, func(i, j int) bool {
		return strings.Compare(rangeKeys[i], rangeKeys[j]) < 0
	})
	assert.Equal(t, expectRangeKeys, rangeKeys)

	expectRows := []string{"Age", "Sex", "active", "address", "birthday",
		"created", "email", "password"}
	rows := userTable.Rows()
	assert.Equal(t, expectRows, rows)
}

func TestInitCustomTableName(t *testing.T) {
	expectName := "custom_name_abc"

	pointerUserTable, err := NewTable(Keyspace{"keyspace", nil}, &CustomUser{})
	assert.NoError(t, err)

	pointerName := pointerUserTable.Name()
	assert.Equal(t, expectName, pointerName)

	valueUserTable, err := NewTable(Keyspace{"keyspace", nil}, CustomUser{})
	assert.NoError(t, err)

	valueName := valueUserTable.Name()
	assert.Equal(t, expectName, valueName)
}
