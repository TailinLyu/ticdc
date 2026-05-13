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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
)

const defaultTargetOwnerDir = ".ticdc/iceberg-target-owners"

var ErrTargetOwnerConflict = stderrors.New(
	"unsupported iceberg target owner conflict: one iceberg target table cannot be shared by multiple changefeeds")

const targetOwnerClaimRetryLimit = 16

type TargetOwnerClaim struct {
	OwnerID      string    `json:"owner_id"`
	CDCClusterID string    `json:"cdc_cluster_id,omitempty"`
	UpstreamID   uint64    `json:"upstream_id,omitempty"`
	Changefeed   string    `json:"changefeed"`
	Identifier   []string  `json:"identifier"`
	CreatedAt    time.Time `json:"created_at"`
}

func NewTargetOwnerClaim(
	cdcClusterID string,
	upstreamID uint64,
	changefeed string,
	identifier []string,
) TargetOwnerClaim {
	return TargetOwnerClaim{
		OwnerID:      TargetOwnerID(cdcClusterID, upstreamID, changefeed),
		CDCClusterID: cdcClusterID,
		UpstreamID:   upstreamID,
		Changefeed:   changefeed,
		Identifier:   append([]string(nil), identifier...),
		CreatedAt:    time.Now().UTC(),
	}
}

func TargetOwnerID(cdcClusterID string, upstreamID uint64, changefeed string) string {
	cdcClusterID = strings.TrimSpace(cdcClusterID)
	if cdcClusterID == "" {
		cdcClusterID = "unknown"
	}
	return "cdc-cluster=" + cdcClusterID +
		";upstream=" + strconv.FormatUint(upstreamID, 10) +
		";changefeed=" + changefeed
}

func ClaimTargetOwner(ctx context.Context, warehouse string, claim TargetOwnerClaim) error {
	return claimTargetOwner(ctx, warehouse, defaultTargetOwnerDir, claim)
}

func ClaimTargetOwnerWithConfig(ctx context.Context, cfg *Config, claim TargetOwnerClaim) error {
	if cfg == nil {
		return fmt.Errorf("nil iceberg sink config")
	}
	return claimTargetOwner(ctx, cfg.Warehouse, cfg.OwnerMarkerPrefix, claim, cfg.externalStorageOptions()...)
}

func claimTargetOwner(
	ctx context.Context,
	warehouse string,
	ownerDir string,
	claim TargetOwnerClaim,
	storageOptions ...util.ExternalStorageOption,
) error {
	if claim.OwnerID == "" {
		return fmt.Errorf("iceberg target owner id is empty")
	}
	if len(claim.Identifier) == 0 {
		return fmt.Errorf("iceberg target identifier is empty")
	}

	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, warehouse, storageOptions...)
	if err != nil {
		return err
	}
	defer store.Close()

	markerPath := targetOwnerMarkerPath(ownerMarkerDir(ownerDir), claim.Identifier)
	hint, err := json.Marshal(claim)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < targetOwnerClaimRetryLimit; attempt++ {
		existing, exists, err := readTargetOwnerClaimIfExists(ctx, store, markerPath)
		if err != nil {
			return err
		}
		if exists {
			return validateTargetOwnerClaim(existing, claim)
		}

		if _, lockErr := storage.TryLockRemote(ctx, store, markerPath, string(hint)); lockErr == nil {
			return nil
		} else {
			existing, exists, readErr := readTargetOwnerClaimIfExists(ctx, store, markerPath)
			if readErr != nil {
				return fmt.Errorf("claim iceberg target owner %q: %w; read existing owner: %v",
					markerPath, lockErr, readErr)
			}
			if exists {
				return validateTargetOwnerClaim(existing, claim)
			}
			lastErr = targetOwnerClaimRetryError(markerPath, lockErr)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(targetOwnerClaimBackoff(attempt, claim.OwnerID)):
		}
	}
	return lastErr
}

func targetOwnerClaimRetryError(markerPath string, lockErr error) error {
	if isTargetOwnerMarkerMissing(lockErr) {
		return fmt.Errorf("claim iceberg target owner %q: owner marker not visible after lock attempt", markerPath)
	}
	return fmt.Errorf("claim iceberg target owner %q: lock attempt failed and owner marker was not visible: %w",
		markerPath, lockErr)
}

func readTargetOwnerClaimIfExists(
	ctx context.Context,
	store storage.ExternalStorage,
	markerPath string,
) (TargetOwnerClaim, bool, error) {
	exists, err := store.FileExists(ctx, markerPath)
	if err != nil {
		return TargetOwnerClaim{}, false, err
	}
	if !exists {
		return TargetOwnerClaim{}, false, nil
	}

	claim, err := readTargetOwnerClaim(ctx, store, markerPath)
	if err != nil {
		if isTargetOwnerMarkerMissing(err) {
			return TargetOwnerClaim{}, false, nil
		}
		return TargetOwnerClaim{}, false, err
	}
	return claim, true, nil
}

func CleanupTargetOwnerClaims(ctx context.Context, warehouse string, ownerID string) error {
	return cleanupWarehouseTargetOwnerClaims(ctx, warehouse, defaultTargetOwnerDir, ownerID)
}

func CleanupTargetOwnerClaimsWithConfig(ctx context.Context, cfg *Config, ownerID string) error {
	if cfg == nil {
		return fmt.Errorf("nil iceberg sink config")
	}
	return cleanupWarehouseTargetOwnerClaims(ctx, cfg.Warehouse, cfg.OwnerMarkerPrefix, ownerID, cfg.externalStorageOptions()...)
}

func cleanupWarehouseTargetOwnerClaims(
	ctx context.Context,
	warehouse string,
	ownerDir string,
	ownerID string,
	storageOptions ...util.ExternalStorageOption,
) error {
	if ownerID == "" {
		return nil
	}
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, warehouse, storageOptions...)
	if err != nil {
		return err
	}
	defer store.Close()

	err = store.WalkDir(ctx, &storage.WalkOption{SubDir: ownerMarkerDir(ownerDir)}, func(path string, _ int64) error {
		claim, err := readTargetOwnerClaim(ctx, store, path)
		if err != nil {
			if isTargetOwnerMarkerMissing(err) {
				return nil
			}
			return err
		}
		if claim.OwnerID != ownerID {
			return nil
		}
		return store.DeleteFile(ctx, path)
	})
	if err != nil && !isTargetOwnerMarkerMissing(err) {
		return err
	}
	return nil
}

func readTargetOwnerClaim(ctx context.Context, store storage.ExternalStorage, markerPath string) (TargetOwnerClaim, error) {
	payload, err := store.ReadFile(ctx, markerPath)
	if err != nil {
		return TargetOwnerClaim{}, err
	}

	var lock storage.LockMeta
	if err := json.Unmarshal(payload, &lock); err != nil {
		return TargetOwnerClaim{}, err
	}
	var claim TargetOwnerClaim
	if err := json.Unmarshal([]byte(lock.Hint), &claim); err != nil {
		return TargetOwnerClaim{}, err
	}
	if claim.OwnerID == "" {
		return TargetOwnerClaim{}, fmt.Errorf("iceberg target owner marker %q has empty owner id", markerPath)
	}
	return claim, nil
}

func validateTargetOwnerClaim(existing TargetOwnerClaim, expected TargetOwnerClaim) error {
	if existing.OwnerID == expected.OwnerID {
		return nil
	}
	return fmt.Errorf("%w: owner %q conflicts with owner %q for target %q",
		ErrTargetOwnerConflict, existing.OwnerID, expected.OwnerID, targetIdentifierKey(expected.Identifier))
}

func targetOwnerMarkerPath(ownerDir string, identifier []string) string {
	sum := sha256.Sum256([]byte(targetIdentifierKey(identifier)))
	return path.Join(ownerMarkerDir(ownerDir), hex.EncodeToString(sum[:])+".lock")
}

func ownerMarkerDir(ownerDir string) string {
	ownerDir = strings.TrimSpace(ownerDir)
	if ownerDir == "" {
		return defaultTargetOwnerDir
	}
	return ownerDir
}

func targetIdentifierKey(identifier []string) string {
	return strings.Join(identifier, "\x1f")
}

func isTargetOwnerMarkerMissing(err error) bool {
	return os.IsNotExist(err) || util.IsNotExistInExtStorage(err)
}

func targetOwnerClaimBackoff(attempt int, ownerID string) time.Duration {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ownerID))
	jitter := time.Duration((h.Sum32()+uint32(attempt)*7)%17) * time.Millisecond
	return time.Duration(attempt+1)*10*time.Millisecond + jitter
}
