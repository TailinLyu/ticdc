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

package iceberg

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultCommitInterval = 60 * time.Second
	defaultBatchRows      = 1024
	defaultTableSuffix    = "_cdc"

	stagingDirContract = "staging-dir must be a local POSIX filesystem path mounted at the same path on every TiCDC capture; it must provide reliable file fsync, directory fsync, atomic rename, and Bolt-compatible mmap/flock locking semantics"
)

// Config contains the local-first Iceberg sink settings parsed from sink-uri.
type Config struct {
	CatalogURI string
	Warehouse  string
	// StagingDir follows stagingDirContract. Shared PVC/RWX/NFS-like paths are
	// supported only when they provide those POSIX and Bolt locking semantics.
	StagingDir     string
	DatabasePrefix string
	TableSuffix    string
	CommitInterval time.Duration
	BatchRows      int

	// Optional HTTP-level overrides for the REST catalog client. Empty values
	// keep iceberg-go defaults.
	CatalogHostHeader string
	CatalogAuthMode   string
	SuppressHeaders   []string

	// Optional AWS SDK knobs for REST catalog SigV4/storage clients.
	AWS AWSOptions

	// AWSRegion and AWSUserAgent are legacy aliases for AWS.Region and the
	// first AWS.UserAgentTags entry. New code should use AWS instead.
	AWSRegion    string
	AWSUserAgent string

	// TableProperties are pass-through Iceberg table properties applied at
	// CreateTable time. Sink-managed properties win on key conflict.
	TableProperties map[string]string
	// OwnerMarkerPrefix is the warehouse-relative directory used for target
	// owner markers. It defaults to defaultTargetOwnerDir.
	OwnerMarkerPrefix string

	// Runtime owner identity. These fields are filled by TiCDC after parsing
	// the sink URI and are used to reject unsupported shared-target writes.
	TiCDCClusterID string
	UpstreamID     uint64
}

// ParseConfig parses the iceberg:// sink URI. By default
// iceberg://host:port maps to an HTTP REST catalog at http://host:port/.
func ParseConfig(uri *url.URL) (*Config, error) {
	if uri == nil {
		return nil, fmt.Errorf("nil iceberg sink URI")
	}
	if strings.ToLower(uri.Scheme) != "iceberg" {
		return nil, fmt.Errorf("invalid iceberg sink scheme %q", uri.Scheme)
	}

	query := uri.Query()
	cfg := &Config{
		CatalogURI:     query.Get("catalog-uri"),
		Warehouse:      query.Get("warehouse"),
		StagingDir:     firstNonEmpty(query.Get("staging-dir"), query.Get("iceberg-staging-dir")),
		DatabasePrefix: firstNonEmpty(query.Get("database-prefix"), query.Get("iceberg-database-prefix")),
		TableSuffix:    firstNonEmpty(query.Get("table-suffix"), query.Get("iceberg-table-suffix"), defaultTableSuffix),
		CommitInterval: defaultCommitInterval,
		BatchRows:      defaultBatchRows,
		CatalogHostHeader: strings.TrimSpace(firstNonEmpty(
			query.Get("catalog-host-header"), query.Get("iceberg-catalog-host-header"))),
		CatalogAuthMode: strings.ToLower(strings.TrimSpace(firstNonEmpty(
			query.Get("auth-mode"), query.Get("iceberg-auth-mode")))),
		SuppressHeaders: splitCommaList(firstNonEmpty(
			query.Get("suppress-headers"), query.Get("iceberg-suppress-headers"))),
	}
	if err := cfg.normalizeCatalogAuthMode(); err != nil {
		return nil, err
	}
	awsOptions, err := parseAWSOptions(firstNonEmpty(
		query.Get("aws"), query.Get("aws-options"),
		query.Get("iceberg-aws"), query.Get("iceberg-aws-options")))
	if err != nil {
		return nil, err
	}
	legacyAWSRegion := strings.TrimSpace(firstNonEmpty(
		query.Get("aws-region"), query.Get("iceberg-aws-region")))
	legacyAWSUserAgent := strings.TrimSpace(firstNonEmpty(
		query.Get("aws-user-agent"), query.Get("iceberg-aws-user-agent")))
	awsOptions.mergeLegacyAliases(legacyAWSRegion, legacyAWSUserAgent)
	cfg.AWS = awsOptions
	cfg.AWSRegion = cfg.AWS.Region
	cfg.AWSUserAgent = cfg.AWS.firstUserAgentTag()
	tableProperties, err := parseTableProperties(firstNonEmpty(
		query.Get("table-properties"), query.Get("iceberg-table-properties")))
	if err != nil {
		return nil, err
	}
	cfg.TableProperties = tableProperties
	ownerMarkerPrefix, err := normalizeOwnerMarkerPrefix(firstNonEmpty(
		query.Get("owner-marker-prefix"), query.Get("iceberg-owner-marker-prefix")))
	if err != nil {
		return nil, err
	}
	cfg.OwnerMarkerPrefix = ownerMarkerPrefix

	if cfg.CatalogURI == "" {
		if uri.Host == "" {
			return nil, fmt.Errorf("iceberg sink URI must include a catalog host or catalog-uri parameter")
		}
		cfg.CatalogURI = (&url.URL{Scheme: "http", Host: uri.Host, Path: "/"}).String()
	}
	if cfg.Warehouse == "" {
		return nil, fmt.Errorf("iceberg sink URI must include warehouse parameter")
	}
	if err := validateWarehouseScheme(cfg.Warehouse); err != nil {
		return nil, err
	}
	if cfg.StagingDir != "" {
		stagingDir, err := localPathFromURI(cfg.StagingDir)
		if err != nil {
			return nil, fmt.Errorf("invalid staging-dir %q: %w", cfg.StagingDir, err)
		}
		cfg.StagingDir = stagingDir
	} else {
		stagingDir, err := defaultStagingPath(cfg.Warehouse)
		if err != nil {
			return nil, fmt.Errorf("iceberg sink URI must include staging-dir when warehouse is not a local path; %s: %w", stagingDirContract, err)
		}
		cfg.StagingDir = stagingDir
	}

	if raw := query.Get("commit-interval"); raw != "" {
		duration, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid commit-interval %q: %w", raw, err)
		}
		if duration <= 0 {
			return nil, fmt.Errorf("commit-interval must be positive")
		}
		cfg.CommitInterval = duration
	}

	if raw := query.Get("batch-rows"); raw != "" {
		rows, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid batch-rows %q: %w", raw, err)
		}
		if rows <= 0 {
			return nil, fmt.Errorf("batch-rows must be positive")
		}
		cfg.BatchRows = rows
	}

	return cfg, nil
}

func normalizeOwnerMarkerPrefix(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultTargetOwnerDir, nil
	}
	if strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("invalid owner-marker-prefix %q: must be a relative warehouse path", raw)
	}
	trimmed := strings.Trim(raw, "/")
	for _, segment := range strings.Split(trimmed, "/") {
		if segment == ".." {
			return "", fmt.Errorf("invalid owner-marker-prefix %q: must not contain parent path segments", raw)
		}
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." || cleaned == "" {
		return "", fmt.Errorf("invalid owner-marker-prefix %q: must not resolve to warehouse root", raw)
	}
	return cleaned, nil
}

// TargetIdentifier maps a TiDB source table to its Iceberg table identifier.
func (c Config) TargetIdentifier(schemaName, tableName string) []string {
	return []string{c.DatabasePrefix + schemaName, tableName + c.TableSuffix}
}

func (c *Config) normalizeCatalogAuthMode() error {
	switch c.CatalogAuthMode {
	case "":
		return nil
	case "none":
		c.SuppressHeaders = appendStringIfMissing(c.SuppressHeaders, "Authorization")
		return nil
	default:
		return fmt.Errorf("invalid auth-mode %q: supported values are none", c.CatalogAuthMode)
	}
}

// EffectiveSuppressHeaders returns the headers the catalog transport should
// remove after iceberg-go has added its defaults.
func (c *Config) EffectiveSuppressHeaders() []string {
	if c == nil {
		return nil
	}
	headers := append([]string(nil), c.SuppressHeaders...)
	if strings.EqualFold(c.CatalogAuthMode, "none") {
		headers = appendStringIfMissing(headers, "Authorization")
	}
	return headers
}

func appendStringIfMissing(values []string, want string) []string {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), want) {
			return values
		}
	}
	return append(values, want)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func splitCommaList(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func parseTableProperties(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "{") {
		properties := make(map[string]string)
		if err := json.Unmarshal([]byte(raw), &properties); err != nil {
			return nil, fmt.Errorf("invalid table-properties JSON: %w", err)
		}
		return properties, nil
	}
	properties := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid table-properties entry %q, expected key=value", part)
		}
		properties[key] = strings.TrimSpace(value)
	}
	return properties, nil
}

func defaultStagingPath(warehouse string) (string, error) {
	warehousePath, err := localPathFromURI(warehouse)
	if err == nil && warehousePath != "" {
		return filepath.Join(warehousePath, ".ticdc-staging"), nil
	}
	return "", err
}

func validateWarehouseScheme(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid warehouse %q: %w", raw, err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "", "file", "s3":
		return nil
	default:
		return fmt.Errorf("unsupported iceberg warehouse scheme %q; supported schemes are local paths, file, and s3", parsed.Scheme)
	}
}

func localPathFromURI(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" {
		if raw == "" {
			return "", fmt.Errorf("path is empty")
		}
		return filepath.Clean(raw), nil
	}
	if parsed.Scheme != "file" {
		return "", fmt.Errorf("must be a local path or file URI")
	}
	if parsed.Host != "" && parsed.Host != "localhost" {
		return "", fmt.Errorf("file URI host %q is not supported", parsed.Host)
	}
	if parsed.Path == "" {
		return "", fmt.Errorf("file URI path is empty")
	}
	return filepath.Clean(filepath.FromSlash(parsed.Path)), nil
}
