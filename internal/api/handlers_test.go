package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"job-queue/internal/api"
	"job-queue/internal/broker"
	"job-queue/internal/metrics"
	"job-queue/internal/tests"
)

func TestMain(m *testing.M) {
	os.Exit(tests.RunRedisMain(m))
}

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	if err := tests.Redis().FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush db: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	b := broker.New(broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
	ts := httptest.NewServer(api.NewServer(b, logger, metrics.New()).Handler())
	t.Cleanup(func() {
		ts.Close()
		b.Close()
	})
	return ts
}

func postJob(t *testing.T, ts *httptest.Server, body string) (int, map[string]interface{}) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post /jobs: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestCreateJob(t *testing.T) {
	ts := newServer(t)

	code, out := postJob(t, ts, `{"type":"demo_task","payload":{"mode":"success"},"max_retries":3}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", code)
	}
	if out["id"] == nil || out["id"] == "" {
		t.Errorf("missing id: %v", out)
	}
	if out["status"] != "pending" {
		t.Errorf("status = %v, want pending", out["status"])
	}
}

func TestCreateJobDedup(t *testing.T) {
	ts := newServer(t)
	body := `{"type":"demo_task","dedup_key":"d-1"}`

	firstCode, first := postJob(t, ts, body)
	secondCode, second := postJob(t, ts, body)

	if firstCode != http.StatusCreated {
		t.Errorf("first status = %d, want 201", firstCode)
	}
	if secondCode != http.StatusOK {
		t.Errorf("second status = %d, want 200", secondCode)
	}
	if first["id"] != second["id"] {
		t.Errorf("dedup ids differ: %v != %v", first["id"], second["id"])
	}
}

func TestCreateJobDelayed(t *testing.T) {
	ts := newServer(t)

	code, out := postJob(t, ts, `{"type":"demo_task","run_after":"2030-01-01T00:00:00Z"}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", code)
	}
	if out["status"] != "scheduled" {
		t.Errorf("status = %v, want scheduled", out["status"])
	}
	if out["id"] == nil || out["id"] == "" {
		t.Errorf("missing id: %v", out)
	}
}

func TestCreateJobValidation(t *testing.T) {
	ts := newServer(t)

	if code, _ := postJob(t, ts, `not-json`); code != http.StatusBadRequest {
		t.Errorf("bad json status = %d, want 400", code)
	}
	if code, _ := postJob(t, ts, `{}`); code != http.StatusBadRequest {
		t.Errorf("missing type status = %d, want 400", code)
	}
}

func TestGetJob(t *testing.T) {
	ts := newServer(t)

	_, out := postJob(t, ts, `{"type":"demo_task","dedup_key":"g-1"}`)
	id, _ := out["id"].(string)
	if id == "" {
		t.Fatal("no job id returned")
	}

	resp, err := http.Get(ts.URL + "/jobs/" + id)
	if err != nil {
		t.Fatalf("get /jobs/%s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var job map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if job["status"] != "pending" || job["type"] != "demo_task" {
		t.Errorf("unexpected job: %v", job)
	}

	resp2, err := http.Get(ts.URL + "/jobs/nope")
	if err != nil {
		t.Fatalf("get missing job: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("missing job status = %d, want 404", resp2.StatusCode)
	}
}

func TestStats(t *testing.T) {
	ts := newServer(t)

	postJob(t, ts, `{"type":"demo_task"}`)

	resp, err := http.Get(ts.URL + "/stats")
	if err != nil {
		t.Fatalf("get /stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var st struct {
		Queues []struct {
			Queue string `json:"queue"`
			Ready int64  `json:"ready"`
		} `json:"queues"`
		Delayed int64 `json:"delayed"`
		Retry   int64 `json:"retry"`
		DLQ     int64 `json:"dlq"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode stats: %v", err)
	}

	found := false
	for _, q := range st.Queues {
		if q.Queue == "default" {
			found = true
			if q.Ready != 1 {
				t.Errorf("default ready = %d, want 1", q.Ready)
			}
		}
	}
	if !found {
		t.Error("default queue missing from stats")
	}
}

func TestRedriveJob(t *testing.T) {
	ctx := context.Background()
	if err := tests.Redis().FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	b := broker.New(broker.Options{RedisOptions: &redis.Options{Addr: tests.RedisAddr()}})
	ts := httptest.NewServer(api.NewServer(b, logger, metrics.New()).Handler())
	defer ts.Close()
	defer b.Close()

	// Create a job, then push it straight into the DLQ.
	_, out := postJob(t, ts, `{"type":"demo_task","max_retries":1}`)
	id, _ := out["id"].(string)
	if id == "" {
		t.Fatal("no job id returned")
	}
	job, err := b.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.EnqueueDLQ(ctx, job, nil); err != nil {
		t.Fatal(err)
	}

	// Redrive it through the API.
	resp, err := http.Post(ts.URL+"/jobs/"+id+"/redrive", "application/json", nil)
	if err != nil {
		t.Fatalf("post redrive: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	got, _ := b.GetJob(ctx, id)
	if got.Status != "pending" {
		t.Errorf("status = %s, want pending", got.Status)
	}
	// The redriven message must be in the stream (plus the original unacked one).
	if length, _ := tests.Redis().XLen(ctx, "queue:default").Result(); length < 1 {
		t.Errorf("stream length = %d, want >= 1", length)
	}

	// Redriving again fails since the job is no longer in the DLQ.
	resp2, err := http.Post(ts.URL+"/jobs/"+id+"/redrive", "application/json", nil)
	if err != nil {
		t.Fatalf("second redrive: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Errorf("second redrive status = %d, want 400", resp2.StatusCode)
	}
}

func TestMetrics(t *testing.T) {
	ts := newServer(t)

	postJob(t, ts, `{"type":"demo_task","dedup_key":"m-1"}`)
	postJob(t, ts, `{"type":"demo_task","dedup_key":"m-1"}`) // dedup, not a new enqueue

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("get /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)

	if !strings.Contains(text, `jobs_enqueued_total{queue="default"} 1`) {
		t.Errorf("jobs_enqueued_total missing/incorrect in:\n%s", text)
	}
	if !strings.Contains(text, `queue_depth{queue="default"} 1`) {
		t.Errorf("queue_depth missing in:\n%s", text)
	}
}
