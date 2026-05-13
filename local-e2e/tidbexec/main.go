// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := flag.String("dsn", "root:@tcp(127.0.0.1:4000)/?multiStatements=true", "TiDB/MySQL DSN")
	query := flag.Bool("query", false, "run query and print rows")
	flag.Parse()

	sqlText := strings.Join(flag.Args(), " ")
	if sqlText == "" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			panic(err)
		}
		sqlText = string(data)
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		panic(err)
	}

	if *query {
		if err := runQuery(db, sqlText); err != nil {
			panic(err)
		}
		return
	}

	if _, err := db.Exec(sqlText); err != nil {
		panic(err)
	}
	fmt.Println("ok")
}

func runQuery(db *sql.DB, sqlText string) error {
	rows, err := db.Query(sqlText)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	fmt.Println(strings.Join(cols, "\t"))

	values := make([]sql.NullString, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		out := make([]string, len(cols))
		for i, value := range values {
			if value.Valid {
				out[i] = value.String
			} else {
				out[i] = "NULL"
			}
		}
		fmt.Println(strings.Join(out, "\t"))
	}
	return rows.Err()
}
