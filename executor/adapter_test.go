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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
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

func (s *testSuiteP2) TestReplacePlaceholder(c *C) {
	tests := []struct {
		query    string
		params   variable.PreparedParams
		expected string
	}{
		{
			query: "select ?;",
			params: variable.PreparedParams{
				types.NewIntDatum(1),
			},
			expected: "select 1;",
		},
		{
			query: "select * from t where a = ? and b in (?, ?);",
			params: variable.PreparedParams{
				types.NewFloat64Datum(1),
				types.NewStringDatum("foo"),
				types.NewDatum(nil),
			},
			expected: "select * from t where a = 1 and b in ('foo', NULL);",
		},
		// Erroneous examples: placeholders and parameters do not match
		{
			query: "select ?, ?;",
			params: variable.PreparedParams{
				types.NewIntDatum(1),
			},
			expected: "select ?, ?;",
		},
		{
			query: "select ?;",
			params: variable.PreparedParams{
				types.NewIntDatum(1),
				types.NewIntDatum(2),
			},
			expected: "select ?;",
		},
	}

	for _, tt := range tests {
		p := parser.New()
		stmtNodes, _, err := p.Parse(tt.query, "", "")
		c.Assert(err, IsNil)
		stmt := &executor.ExecStmt{Text: tt.query, StmtNode: stmtNodes[0]}
		obtained := executor.ReplacePlaceholder(stmt, tt.params)
		c.Assert(obtained, Equals, tt.expected)
	}
}
