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

package locale2e

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalE2EScriptsAreParseable(t *testing.T) {
	scripts := []string{
		"iceberg_case_lib.sh",
		"run_s03_append_exit_replay.sh",
		"run_s05_stage_exit_replay.sh",
		"run_s09_catalog_outage_recovery.sh",
		"run_s11_append_error_replay.sh",
		"run_s12_high_volume_drain.sh",
		"run_s15_owner_guard.sh",
		"run_s16_minio_owner_marker.sh",
		"run_s17_schema_unsupported.sh",
		"run_s20_rolling_restart.sh",
	}
	for _, script := range scripts {
		t.Run(script, func(t *testing.T) {
			info, err := os.Stat(script)
			require.NoError(t, err)
			require.False(t, info.IsDir())

			cmd := exec.Command("bash", "-n", script)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}
}
