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
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type tableResult struct {
	Table              string `json:"table"`
	Inserted           int64  `json:"inserted"`
	Updated            int64  `json:"updated"`
	Deleted            int64  `json:"deleted"`
	InsertDurationMS   int64  `json:"insert_duration_ms"`
	MaxRowsPerClientMS int64  `json:"max_rows_per_client_ms"`
	ActiveClientMS     int64  `json:"active_client_ms"`
}

type workloadResult struct {
	Database string        `json:"database"`
	Tables   []tableResult `json:"tables"`
	Total    tableResult   `json:"total"`
}

func main() {
	dsn := flag.String("dsn", "root:@tcp(127.0.0.1:4000)/?multiStatements=true", "TiDB/MySQL DSN")
	dbName := flag.String("db", "ticdc_iceberg_e2e", "database name")
	tableList := flag.String("tables", "orders", "comma-separated table names")
	rows := flag.Int64("rows", 1000, "rows to insert per table")
	workers := flag.Int("workers", 16, "concurrent insert workers per table")
	batch := flag.Int("batch", 50, "rows per insert statement")
	batchDelay := flag.Duration("batch-delay", 0, "optional delay after each insert batch")
	idStride := flag.Int64("id-stride", 100, "distance between generated primary keys")
	splitRegions := flag.Int("split-regions", 16, "SPLIT TABLE region count per table; 0 disables split")
	updateEvery := flag.Int64("update-every", 10, "update every Nth generated row; 0 disables updates")
	deleteEvery := flag.Int64("delete-every", 15, "delete every Nth generated row; 0 disables deletes")
	reset := flag.Bool("reset", false, "drop and recreate tables before writing")
	prepareOnly := flag.Bool("prepare-only", false, "create and split tables, then exit without writing rows")
	mutationsOnly := flag.Bool("mutations-only", false, "skip inserts and only run update/delete waves")
	flag.Parse()

	tables := parseTables(*tableList)
	if len(tables) == 0 {
		fatalf("no tables supplied")
	}
	if *rows < 0 || *workers <= 0 || *batch <= 0 || *idStride <= 0 {
		fatalf("invalid workload sizing flags")
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fatalf("open TiDB connection: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns((*workers * len(tables)) + 8)
	db.SetMaxIdleConns((*workers * len(tables)) + 8)

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		fatalf("ping TiDB: %v", err)
	}

	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(*dbName)); err != nil {
		fatalf("create database: %v", err)
	}
	for _, table := range tables {
		if *reset {
			if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName(*dbName, table)); err != nil {
				fatalf("drop table %s: %v", table, err)
			}
		}
		if err := createTable(ctx, db, *dbName, table); err != nil {
			fatalf("create table %s: %v", table, err)
		}
		if *splitRegions > 0 {
			if err := splitTable(ctx, db, *dbName, table, *rows, *idStride, *splitRegions); err != nil {
				fatalf("split table %s: %v", table, err)
			}
		}
	}

	result := workloadResult{Database: *dbName}
	if *prepareOnly {
		writeJSON(result)
		return
	}

	for _, table := range tables {
		tableResult := tableResult{Table: table}
		if !*mutationsOnly {
			inserted, insertStats, err := insertRows(ctx, db, *dbName, table, *rows, *workers, *batch, *idStride, *batchDelay)
			if err != nil {
				fatalf("insert table %s: %v", table, err)
			}
			tableResult.Inserted = inserted
			tableResult.InsertDurationMS = insertStats.durationMS
			tableResult.MaxRowsPerClientMS = insertStats.maxRowsPerMS
			tableResult.ActiveClientMS = insertStats.activeMS
		}
		if *updateEvery > 0 {
			updated, err := execMutation(ctx, db, fmt.Sprintf(
				"UPDATE %s SET amount = amount + 7.5, paid = 1, note = CONCAT(note, '-u') WHERE id %% ? = 0",
				tableName(*dbName, table),
			), (*updateEvery)*(*idStride))
			if err != nil {
				fatalf("update table %s: %v", table, err)
			}
			tableResult.Updated = updated
		}
		if *deleteEvery > 0 {
			deleted, err := execMutation(ctx, db, fmt.Sprintf(
				"DELETE FROM %s WHERE id %% ? = 0",
				tableName(*dbName, table),
			), (*deleteEvery)*(*idStride))
			if err != nil {
				fatalf("delete table %s: %v", table, err)
			}
			tableResult.Deleted = deleted
		}
		result.Tables = append(result.Tables, tableResult)
		result.Total.Inserted += tableResult.Inserted
		result.Total.Updated += tableResult.Updated
		result.Total.Deleted += tableResult.Deleted
		result.Total.InsertDurationMS += tableResult.InsertDurationMS
		if tableResult.MaxRowsPerClientMS > result.Total.MaxRowsPerClientMS {
			result.Total.MaxRowsPerClientMS = tableResult.MaxRowsPerClientMS
		}
		result.Total.ActiveClientMS += tableResult.ActiveClientMS
	}
	result.Total.Table = "TOTAL"
	writeJSON(result)
}

type insertStats struct {
	durationMS   int64
	maxRowsPerMS int64
	activeMS     int64
}

func insertRows(
	ctx context.Context,
	db *sql.DB,
	dbName string,
	table string,
	rows int64,
	workers int,
	batch int,
	idStride int64,
	batchDelay time.Duration,
) (int64, insertStats, error) {
	if rows == 0 {
		return 0, insertStats{}, nil
	}

	var next int64
	var inserted int64
	var mu sync.Mutex
	rowsByMS := map[int64]int64{}
	started := time.Now()

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				start := atomic.AddInt64(&next, int64(batch)) - int64(batch) + 1
				if start > rows {
					return
				}
				end := start + int64(batch) - 1
				if end > rows {
					end = rows
				}
				count, err := insertBatch(ctx, db, dbName, table, start, end, idStride, workerID)
				if err != nil {
					errCh <- err
					return
				}
				if batchDelay > 0 {
					time.Sleep(batchDelay)
				}
				atomic.AddInt64(&inserted, count)
				ms := time.Since(started).Milliseconds()
				mu.Lock()
				rowsByMS[ms] += count
				mu.Unlock()
			}
		}(workerID)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return inserted, insertStats{}, err
		}
	}

	stats := insertStats{durationMS: time.Since(started).Milliseconds()}
	mu.Lock()
	for _, count := range rowsByMS {
		if count > stats.maxRowsPerMS {
			stats.maxRowsPerMS = count
		}
		stats.activeMS++
	}
	mu.Unlock()
	return inserted, stats, nil
}

func insertBatch(
	ctx context.Context,
	db *sql.DB,
	dbName string,
	table string,
	start int64,
	end int64,
	idStride int64,
	workerID int,
) (int64, error) {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(tableName(dbName, table))
	builder.WriteString(" (id, customer, amount, paid, note) VALUES ")

	args := make([]any, 0, int(end-start+1)*5)
	for seq := start; seq <= end; seq++ {
		if seq > start {
			builder.WriteString(",")
		}
		builder.WriteString("(?, ?, ?, ?, ?)")
		id := seq * idStride
		args = append(args,
			id,
			fmt.Sprintf("customer-%s-%08d", table, seq),
			float64(seq%100000)/10.0,
			seq%2,
			fmt.Sprintf("worker-%03d-row-%08d", workerID, seq),
		)
	}
	result, err := db.ExecContext(ctx, builder.String(), args...)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func execMutation(ctx context.Context, db *sql.DB, stmt string, args ...any) (int64, error) {
	result, err := db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func createTable(ctx context.Context, db *sql.DB, dbName string, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  id BIGINT PRIMARY KEY,
  customer VARCHAR(96) NOT NULL,
  amount DECIMAL(20, 2) NOT NULL,
  paid TINYINT NOT NULL,
  note VARCHAR(160) NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)`, tableName(dbName, table)))
	return err
}

func splitTable(ctx context.Context, db *sql.DB, dbName string, table string, rows int64, idStride int64, regions int) error {
	upper := (rows + 2) * idStride
	minUpper := int64(regions) * 2000
	if upper < minUpper {
		upper = minUpper
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"SPLIT TABLE %s BETWEEN (0) AND (%d) REGIONS %d",
		tableName(dbName, table),
		upper,
		regions,
	))
	return err
}

func parseTables(input string) []string {
	parts := strings.Split(input, ",")
	tables := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		table := strings.TrimSpace(part)
		if table == "" {
			continue
		}
		if _, ok := seen[table]; ok {
			continue
		}
		seen[table] = struct{}{}
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func tableName(dbName string, table string) string {
	return quoteIdent(dbName) + "." + quoteIdent(table)
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func writeJSON(value any) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		fatalf("encode output: %v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
