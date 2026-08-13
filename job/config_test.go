package job_test

import (
	"testing"
	"time"

	"github.com/241x/zero-kit/job"

	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	require.NoError(t, job.DefaultConfig().Validate())
	require.Error(t, job.Config{}.Validate())

	tests := []struct {
		name string
		cfg  job.Config
	}{
		{"zero concurrency", job.DefaultConfig().WithConcurrency(0)},
		{"zero heartbeat", job.DefaultConfig().WithHeartbeatInterval(0)},
		{"zero stale timeout", job.DefaultConfig().WithStaleTimeout(0)},
		{"zero recover interval", job.DefaultConfig().WithRecoverInterval(0)},
		{"negative retry delay", job.DefaultConfig().WithRetryDelay(-time.Second)},
		{"negative retry max delay", job.DefaultConfig().WithRetryMaxDelay(-time.Second)},
		{"zero cleanup interval", job.DefaultConfig().WithRetention(time.Hour).WithCleanupInterval(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.cfg.Validate())
		})
	}
}
