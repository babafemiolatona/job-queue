package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"job-queue/internal/queue"
	"job-queue/internal/worker"
)

func DemoTask() worker.Handler {
	return func(ctx context.Context, j *queue.Job) error {
		var spec struct {
			Mode string `json:"mode"`
			N    int    `json:"n"`
		}
		if len(j.Payload) > 0 {
			_ = json.Unmarshal(j.Payload, &spec)
		}

		switch spec.Mode {
		case "slow":
			time.Sleep(time.Duration(spec.N) * time.Second)
			return nil
		case "fail_times":
			if j.Attempt < spec.N {
				return fmt.Errorf("injected failure (attempt %d/%d)", j.Attempt+1, spec.N)
			}
			return nil
		case "always_fail":
			return errors.New("always fails")
		default:
			return nil
		}
	}
}
