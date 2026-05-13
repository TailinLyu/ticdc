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
	stdio "io"
	"net/http"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestAWSOptionsBuildStorageBackendOptions(t *testing.T) {
	pathStyleAccess := false
	cfg := &Config{AWS: AWSOptions{
		Region:          "us-east-1",
		Endpoint:        "https://s3.example.com",
		PathStyleAccess: &pathStyleAccess,
	}}

	opts := cfg.s3BackendOptions()
	require.NotNil(t, opts)
	require.Equal(t, "us-east-1", opts.S3.Region)
	require.Equal(t, "https://s3.example.com", opts.S3.Endpoint)
	require.False(t, opts.S3.ForcePathStyle)
}

func TestAWSOptionsPreservesBRStorageDefaultPathStyle(t *testing.T) {
	cfg := &Config{AWS: AWSOptions{Region: "us-west-2"}}

	opts := cfg.s3BackendOptions()
	require.NotNil(t, opts)
	require.Equal(t, "us-west-2", opts.S3.Region)
	require.True(t, opts.S3.ForcePathStyle)
}

func TestAWSOptionsBuildIcebergS3Properties(t *testing.T) {
	pathStyleAccess := false
	cfg := &Config{AWS: AWSOptions{
		Region:          "us-east-1",
		Endpoint:        "https://s3.example.com",
		PathStyleAccess: &pathStyleAccess,
	}}

	props := cfg.IcebergS3Properties()
	require.Equal(t, "us-east-1", props[icebergio.S3Region])
	require.Equal(t, "https://s3.example.com", props[icebergio.S3EndpointURL])
	require.Equal(t, "true", props[icebergio.S3ForceVirtualAddressing])
}

func TestAWSUserAgentTransportAppendsTags(t *testing.T) {
	var seen string
	transport := &awsUserAgentTransport{
		tags: []string{"iceberg-ready", "my-app/1.2"},
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			seen = req.Header.Get("User-Agent")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       stdio.NopCloser(http.NoBody),
			}, nil
		}),
	}
	req, err := http.NewRequest(http.MethodGet, "https://s3.example.com/bucket/key", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "aws-sdk-go/1.0")

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, "aws-sdk-go/1.0 iceberg-ready my-app/1.2", seen)
	require.Equal(t, "aws-sdk-go/1.0", req.Header.Get("User-Agent"))
}
