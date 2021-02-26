// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/stretchr/testify/assert"
)

func (s *testSuiteP2) TestQueryTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	costTime := time.Since(tk.Se.GetSessionVars().StartTime)
	c.Assert(costTime < 1*time.Second, IsTrue)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (1), (1), (1), (1)")
	tk.MustExec("select * from t t1 join t t2 on t1.a = t2.a")

	costTime = time.Since(tk.Se.GetSessionVars().StartTime)
	c.Assert(costTime < 1*time.Second, IsTrue)
}

// func TestExtractPlaceholderPosisions(t *testing.T) {
// 	sql := "select * from t where a = ? and b in (?, ?);"
// 	p := parser.New()
// 	stmtNodes, _, _ := p.Parse(sql, "", "")
// 		px := &positionExtractor{}
// 	positions := executor.Extract(stmtNodes[0])
// 	assert.Equal(t, []int{26, 38, 41}, positions)
// }

func TestFormatPreparedStmt(t *testing.T) {
	params := variable.PreparedParams{
		types.NewIntDatum(1),
		types.NewFloat64Datum(2),
		types.NewDatum(nil),
	}
	sql := "select ?, ?, ?;"
	p := parser.New()

	stmtNodes, _, _ := p.Parse(sql, "", "")

	stmt := &executor.ExecStmt{Text: sql, StmtNode: stmtNodes[0]}
	preparedSQL := executor.FormatPreparedStmt(stmt, params)
	assert.Equal(t, "select 1, 2, NULL;", preparedSQL)

}

// mustParseTimeIntoDatum is similar to ParseTime but panic if any error occurs.
func mustParseTimeIntoDatum(s string, tp byte, fsp int8) (d types.Datum) {
	t, err := types.ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, s, tp, fsp)
	if err != nil {
		panic("ParseTime fail")
	}
	d.SetMysqlTime(t)
	return
}
