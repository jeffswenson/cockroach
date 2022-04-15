package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/balancer"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tenantDialers struct {
	balancer       *balancer.Balancer
	directoryCache tenant.DirectoryCache
	tlsConfig      *tls.Config
	mu             struct {
		sync.Mutex
		dialers map[roachpb.TenantID]*tenantDialer
	}
}

func NewTenantDialers(balancer *balancer.Balancer, directoryCache tenant.DirectoryCache, tlsConfig *tls.Config) *tenantDialers {
	result := &tenantDialers{
		balancer:       balancer,
		directoryCache: directoryCache,
		tlsConfig:      tlsConfig,
	}
	result.mu.dialers = make(map[roachpb.TenantID]*tenantDialer)
	return result
}

func (dialers *tenantDialers) GetDialer(tenant roachpb.TenantID, clusterName string) *tenantDialer {
	dialers.mu.Lock()
	defer dialers.mu.Unlock()

	dialer, ok := dialers.mu.dialers[tenant]
	if ok {
		return dialer
	}

	dialer = &tenantDialer{
		clusterName:    clusterName,
		tenantID:       tenant,
		balancer:       dialers.balancer,
		directoryCache: dialers.directoryCache,
	}
	dialer.mu.connectionCounts = make(map[string]int)

	return dialer
}

type tenantDialer struct {
	tenantID       roachpb.TenantID
	clusterName    string
	balancer       *balancer.Balancer
	directoryCache tenant.DirectoryCache
	tlsConfig      *tls.Config
	mu             struct {
		sync.Mutex
		connectionCounts map[string]int
	}
}

func (td *tenantDialer) DialTenant(ctx context.Context) (net.Conn, error) {
	addr, err := td.lookupAddr(ctx)
	if err != nil {
		return nil, err
	}

	td.addToCount(addr, 1)

	conn, err := td.dialSQLServer(addr)
	if err != nil {
		td.addToCount(addr, -1)
	}

	return &onConnectionClose{
		Conn: conn,
		onClose: func() {
			td.addToCount(addr, -1)
		},
	}, nil
}

func (td *tenantDialer) addToCount(addr string, value int) {
	td.mu.Lock()
	defer td.mu.Unlock()

	count := td.mu.connectionCounts[addr] + value
	if count <= 0 {
		delete(td.mu.connectionCounts, addr)
	} else {
		td.mu.connectionCounts[addr] = count
	}
}

// lookupAddr returns an address (that must include both host and port)
// pointing to one of the SQL pods for the tenant associated with this
// connector.
//
// This will be called within an infinite backoff loop. If an error is
// transient, this will return an error that has been marked with
// errRetryConnectorSentinel (i.e. markAsRetriableConnectorError).
func (td *tenantDialer) lookupAddr(ctx context.Context) (string, error) {
	// Lookup tenant in the directory cache. Once we have retrieve the list of
	// pods, use the Balancer for load balancing.
	pods, err := td.directoryCache.LookupTenantPods(ctx, td.tenantID, td.clusterName)
	switch {
	case err == nil:
		runningPods := make([]*tenant.Pod, 0, len(pods))
		for _, pod := range pods {
			if pod.State == tenant.RUNNING {
				runningPods = append(runningPods, pod)
			}
		}
		pod, err := td.balancer.SelectTenantPod(runningPods)
		if err != nil {
			// This should never happen because LookupTenantPods ensured that
			// there should be at least one RUNNING pod. Mark it as a retriable
			// connection anyway.
			return "", markAsRetriableConnectorError(err)
		}
		return pod.Addr, nil

	case status.Code(err) == codes.FailedPrecondition:
		if st, ok := status.FromError(err); ok {
			return "", newErrorf(codeUnavailable, "%v", st.Message())
		}
		return "", newErrorf(codeUnavailable, "unavailable")

	case status.Code(err) == codes.NotFound:
		return "", newErrorf(codeParamsRoutingFailed,
			"cluster %s-%d not found", td.clusterName, td.tenantID.ToUint64())

	default:
		return "", markAsRetriableConnectorError(err)
	}
}

// dialSQLServer dials the given address for the SQL pod, and forwards the
// startup message to it. If the connector specifies a TLS connection, it will
// also attempt to upgrade the PG connection to use TLS.
//
// This will be called within an infinite backoff loop. If an error is
// transient, this will return an error that has been marked with
// errRetryConnectorSentinel (i.e. markAsRetriableConnectorError).
func (td *tenantDialer) dialSQLServer(serverAddr string) (net.Conn, error) {
	// Use a TLS config if one was provided. If TLSConfig is nil, Clone will
	// return nil.
	tlsConf := td.tlsConfig.Clone()
	if tlsConf != nil {
		// serverAddr will always have a port. We use an empty string as the
		// default port as we only care about extracting the host.
		outgoingHost, _, err := addr.SplitHostPort(serverAddr, "" /* defaultPort */)
		if err != nil {
			return nil, err
		}
		// Always set ServerName. If InsecureSkipVerify is true, this will
		// be ignored.
		tlsConf.ServerName = outgoingHost
	}

	conn, err := net.DialTimeout("tcp", serverAddr, time.Second*5)
	if err != nil {
		return nil, newErrorf(
			codeBackendDown, "unable to reach backend SQL server: %v", err,
		)
	}
	conn, err = sslOverlay(conn, tlsConf)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

type onConnectionClose struct {
	net.Conn
	onClose func()
	once    sync.Once
}

func (cc *onConnectionClose) Close() error {
	cc.once.Do(cc.onClose)
	return cc.Conn.Close()
}
