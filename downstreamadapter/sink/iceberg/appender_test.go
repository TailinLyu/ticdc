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
	stderrors "errors"
	"testing"

	iceberggo "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

type namespaceProbeCatalog struct {
	createErr error
	exists    bool
	checkErr  error
}

func (c namespaceProbeCatalog) CreateNamespace(
	context.Context,
	icebergtable.Identifier,
	iceberggo.Properties,
) error {
	return c.createErr
}

func (c namespaceProbeCatalog) CheckNamespaceExists(
	context.Context,
	icebergtable.Identifier,
) (bool, error) {
	return c.exists, c.checkErr
}

func TestEnsureNamespaceContinuesWhenCreateErrorLeavesNamespaceExisting(t *testing.T) {
	err := ensureNamespace(context.Background(), namespaceProbeCatalog{
		createErr: stderrors.New("sqlite catalog busy"),
		exists:    true,
	}, icebergtable.Identifier{"test"})

	require.NoError(t, err)
}

func TestEnsureNamespaceReturnsCreateErrorWhenNamespaceStillMissing(t *testing.T) {
	err := ensureNamespace(context.Background(), namespaceProbeCatalog{
		createErr: stderrors.New("sqlite catalog busy"),
		exists:    false,
	}, icebergtable.Identifier{"test"})

	require.Error(t, err)
	require.Contains(t, err.Error(), "sqlite catalog busy")
}
