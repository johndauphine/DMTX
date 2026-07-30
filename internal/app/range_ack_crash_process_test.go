package app

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage2RangeAckHelperEnv = "DMTX_STAGE2_RANGE_ACK_HELPER"
	stage2RangeAckEventEnv  = "DMTX_STAGE2_RANGE_ACK_EVENT"
)

func TestStage2SQLiteWriteBeforeRangeAckHelperProcess(t *testing.T) {
	if os.Getenv(stage2RangeAckHelperEnv) != "1" {
		return
	}
	configPath := os.Getenv(stage1HelperConfigEnv)
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	observer := &stage2WriteBeforeRangeAckObserver{
		tableCheckpointObserver: tableCheckpointObserver{
			store: state.SQLiteStore{Path: configPath + ".state.db"},
			runID: os.Getenv(stage1HelperRunIDEnv),
		},
		eventPath: os.Getenv(stage2RangeAckEventEnv),
	}
	if _, err := migrate.SQLiteToSQLiteWithObserver(
		context.Background(),
		cfg,
		observer,
	); err != nil {
		t.Fatalf("migration returned before hard kill: %v", err)
	}
	t.Fatal("migration completed before hard kill")
}

type stage2WriteBeforeRangeAckObserver struct {
	tableCheckpointObserver
	eventPath string
	triggered bool
}

func (observer *stage2WriteBeforeRangeAckObserver) BeforeSQLiteChunkAcknowledge(
	ctx context.Context,
	info migrate.SQLiteChunkInfo,
	receipt migrate.WriteReceipt,
) error {
	if observer.triggered || info.Sequence != 0 ||
		receipt.Certainty != migrate.CommitDurable {
		return nil
	}
	observer.triggered = true
	if err := os.WriteFile(observer.eventPath, []byte("target-committed"), 0o600); err != nil {
		return err
	}
	return waitForParentHardKill(ctx)
}

func stage2RangeAckHelperCommand(
	configPath,
	runID,
	eventPath string,
) *exec.Cmd {
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestStage2SQLiteWriteBeforeRangeAckHelperProcess$",
	)
	command.Env = append(
		os.Environ(),
		stage2RangeAckHelperEnv+"=1",
		stage1HelperConfigEnv+"="+configPath,
		stage1HelperRunIDEnv+"="+runID,
		stage2RangeAckEventEnv+"="+eventPath,
	)
	return command
}
