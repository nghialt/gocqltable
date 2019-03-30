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
	Created  time.Time `cql:"created"`
}

func TestGetCreateSchema(t *testing.T) {

	expectSchema := "CREATE TABLE \"keyspace\".\"users\" (\nSex int,\nactive boolean,\naddress varchar,\nbirthday varchar,\ncreated timestamp,\nemail varchar,\npassword varchar,\nPRIMARY KEY ((active, birthday), address, Sex))"

	userTable := NewTable(Keyspace{"keyspace", nil}, User{})
	schema, err := userTable.GetCreateSchema()

	assert.NoError(t, err)
	assert.Equal(t, expectSchema, strings.TrimSpace(schema))
}

func TestInitRowKeys(t *testing.T) {

	expectRowKeys := []string{"active", "birthday"}

	userTable := NewTable(Keyspace{"keyspace", nil}, User{})
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

	expectRows := []string{"Sex", "active", "address", "birthday",
		"created", "email", "password"}
	rows := userTable.Rows()
	assert.Equal(t, expectRows, rows)
}
