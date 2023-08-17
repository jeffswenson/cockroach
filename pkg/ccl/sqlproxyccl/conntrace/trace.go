package conntrace

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type Trace struct {
	maxLogs int
	mu struct {
		syncutil.Mutex
		
		startedAt time.Time
		connectedAt time.Time
		authenticatedAt time.Time
		closedAt time.Time

		tenant string
		clusterName string

		dialAttempts int32

		transferAttempts int32
		transferFailed int32
		lastTransferAttemptAt time.Time

		clientMessages int64
		clientBytes int64
		serverMessages int64
		serverBytes int64

		droppedLogs int64
		logs []traceLog
	}
}

type traceLog struct {
	time time.Time
	serverity string
	message string
}

func NewTrace(clientAddress string) *Trace {
	result := Trace {
		maxLogs: 10,		
	}
	result.mu.startedAt = timeutil.Now()
	return &result
}

func (t *Trace) Write(ctx context.Context) {
	// Write the trace to the log
}

func (t *Trace) String() {

}

func (t *Trace) Log(severity string, message string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logLocked(severity, message, args...)
}

func (t *Trace) ConnectionAge() time.Duration {
	return timeutil.Since(t.mu.startedAt)
}

func (t *Trace) logLocked(severity string, message string, args ...any) {

}

func (t *Trace) SetTenant(tenant string, clusterName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.tenant = tenant
	t.mu.clusterName = clusterName
}

func (t *Trace) RecordCancelRequest(err error) {
		// Lots of noise from this log indicates that somebody is spamming
		// fake cancel requests.
		// log.Warningf(
		// 	ctx, "could not handle cancel request from client %s: %v",
		// 	incomingConn.RemoteAddr().String(), err,
		// )
}

func (t *Trace) RecordDialAttempt(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.dialAttempts += 1
	if err != nil {
		t.logLocked("ERROR","connecting to server: %s", err)
	}
}

func (t *Trace) RecordConnected(serverAddress string, serverStartedAt time.Time) {

}

func (t *Trace) RecordClosed(err error) {

}

func (t *Trace) RecordClientTraffic(messages int64, bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.clientBytes += bytes
	t.mu.clientMessages += messages
}

func (t *Trace) RecordServerTraffic(messages int64, bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.serverBytes += bytes
	t.mu.serverMessages += messages
}
