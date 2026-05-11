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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// AWSOptions contains AWS/S3 options shared by every S3 client the Iceberg sink creates.
type AWSOptions struct {
	Region          string   `json:"region"`
	Endpoint        string   `json:"endpoint"`
	PathStyleAccess *bool    `json:"path-style-access"`
	UserAgentTags   []string `json:"user-agent-tags"`
}

func parseAWSOptions(raw string) (AWSOptions, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return AWSOptions{}, nil
	}
	var opts AWSOptions
	if err := json.Unmarshal([]byte(raw), &opts); err != nil {
		return AWSOptions{}, fmt.Errorf("invalid aws options: %w", err)
	}
	if err := opts.normalize(); err != nil {
		return AWSOptions{}, err
	}
	return opts, nil
}

func (o *AWSOptions) normalize() error {
	o.Region = strings.TrimSpace(o.Region)
	o.Endpoint = strings.TrimSpace(o.Endpoint)
	if len(o.UserAgentTags) == 0 {
		return nil
	}
	tags := make([]string, 0, len(o.UserAgentTags))
	seen := make(map[string]struct{}, len(o.UserAgentTags))
	for _, tag := range o.UserAgentTags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			return fmt.Errorf("invalid aws user-agent-tags: empty tag")
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}
	o.UserAgentTags = tags
	return nil
}

func (o *AWSOptions) mergeLegacyAliases(region string, userAgent string) {
	if o.Region == "" {
		o.Region = strings.TrimSpace(region)
	}
	if len(o.UserAgentTags) == 0 {
		userAgent = strings.TrimSpace(userAgent)
		if userAgent != "" {
			o.UserAgentTags = []string{userAgent}
		}
	}
}

func (o AWSOptions) firstUserAgentTag() string {
	if len(o.UserAgentTags) == 0 {
		return ""
	}
	return o.UserAgentTags[0]
}

func (o AWSOptions) needsAWSConfig() bool {
	return o.Region != "" || len(o.UserAgentTags) > 0
}

func (o AWSOptions) hasS3BackendOptions() bool {
	return o.Region != "" || o.Endpoint != "" || o.PathStyleAccess != nil
}

func (c *Config) effectiveAWSOptions() AWSOptions {
	if c == nil {
		return AWSOptions{}
	}
	opts := c.AWS
	opts.mergeLegacyAliases(c.AWSRegion, c.AWSUserAgent)
	return opts
}

// BuildAWSConfig builds the AWS SDK v2 config used by iceberg-go REST/S3 paths.
func (c *Config) BuildAWSConfig(ctx context.Context) (*aws.Config, error) {
	awsOptions := c.effectiveAWSOptions()
	if !awsOptions.needsAWSConfig() {
		return nil, nil
	}
	loadOpts := make([]func(*awsconfig.LoadOptions) error, 0, 2)
	if awsOptions.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(awsOptions.Region))
	}
	if len(awsOptions.UserAgentTags) > 0 {
		apiOptions := make([]func(*smithymiddleware.Stack) error, 0, len(awsOptions.UserAgentTags))
		for _, tag := range awsOptions.UserAgentTags {
			apiOptions = append(apiOptions, awsmiddleware.AddUserAgentKey(tag))
		}
		loadOpts = append(loadOpts, awsconfig.WithAPIOptions(apiOptions))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// IcebergS3Properties returns S3 properties passed into iceberg-go catalog/table IO.
func (c *Config) IcebergS3Properties() map[string]string {
	awsOptions := c.effectiveAWSOptions()
	if !awsOptions.hasS3BackendOptions() {
		return nil
	}
	props := make(map[string]string)
	if awsOptions.Region != "" {
		props[icebergio.S3Region] = awsOptions.Region
	}
	if awsOptions.Endpoint != "" {
		props[icebergio.S3EndpointURL] = awsOptions.Endpoint
	}
	if awsOptions.PathStyleAccess != nil {
		props[icebergio.S3ForceVirtualAddressing] = strconv.FormatBool(!*awsOptions.PathStyleAccess)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

func (c *Config) externalStorageOptions() []util.ExternalStorageOption {
	if c == nil {
		return nil
	}
	opts := make([]util.ExternalStorageOption, 0, 2)
	if backendOpts := c.s3BackendOptions(); backendOpts != nil {
		opts = append(opts, util.WithExternalStorageBackendOptions(backendOpts))
	}
	if client := c.awsHTTPClient(); client != nil {
		opts = append(opts, util.WithExternalStorageHTTPClient(client))
	}
	return opts
}

func (c *Config) s3BackendOptions() *storage.BackendOptions {
	awsOptions := c.effectiveAWSOptions()
	if !awsOptions.hasS3BackendOptions() {
		return nil
	}
	opts := &storage.BackendOptions{}
	opts.S3.Region = awsOptions.Region
	opts.S3.Endpoint = awsOptions.Endpoint
	opts.S3.ForcePathStyle = true
	if awsOptions.PathStyleAccess != nil {
		opts.S3.ForcePathStyle = *awsOptions.PathStyleAccess
	}
	return opts
}

func (c *Config) awsHTTPClient() *http.Client {
	awsOptions := c.effectiveAWSOptions()
	if len(awsOptions.UserAgentTags) == 0 {
		return nil
	}
	return &http.Client{Transport: &awsUserAgentTransport{
		tags: append([]string(nil), awsOptions.UserAgentTags...),
	}}
}

type awsUserAgentTransport struct {
	tags []string
	base http.RoundTripper
}

func (t *awsUserAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if tags := strings.Join(t.tags, " "); tags != "" {
		if existing := clone.Header.Get("User-Agent"); existing != "" {
			clone.Header.Set("User-Agent", existing+" "+tags)
		} else {
			clone.Header.Set("User-Agent", tags)
		}
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}
