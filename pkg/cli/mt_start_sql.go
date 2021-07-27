// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

var mtStartSQLCmd = &cobra.Command{
	Use:   "start-sql",
	Short: "start a standalone SQL server",
	Long: `
Start a standalone SQL server.

This functionality is **experimental** and for internal use only.

The following certificates are required:

- ca.crt, node.{crt,key}: CA cert and key pair for serving the SQL endpoint.
  Note that under no circumstances should the node certs be shared with those of
  the same name used at the KV layer, as this would pose a severe security risk.
- ca-client-tenant.crt, client-tenant.X.{crt,key}: CA cert and key pair for
  authentication and authorization with the KV layer (as tenant X).
- ca-server-tenant.crt: to authenticate KV layer.

                 ca.crt        ca-client-tenant.crt        ca-server-tenant.crt
user ---------------> sql server ----------------------------> kv
 client.Y.crt    node.crt      client-tenant.X.crt         server-tenant.crt
 client.Y.key    node.key      client-tenant.X.key         server-tenant.key

Note that CA certificates need to be present on the "other" end of the arrow as
well unless it can be verified using a trusted root certificate store. That is,

- ca.crt needs to be passed in the Postgres connection string (sslrootcert) if
  sslmode=verify-ca.
- ca-server-tenant.crt needs to be present on the SQL server.
- ca-client-tenant.crt needs to be present on the KV server.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runStartSQL),
}

const (
	starterFileLimit   = 5 << 20 // 5 MiB
	checkpointFileName = "STARTER_CHECKPOINT"
)

func atomicWriteFile(filename string, data []byte) error {
	// TODO make this atomic
	return ioutil.WriteFile(filename, data, 0400)
}

// Hooks for testing.
var (
	unixExec = unix.Exec

	tenantDataDir = "/cockroach/tenant-data"
)

var options struct {
	listenAddress string
	cert          string
	key           string
	insecure      bool

	// CRDB-specific options.
	cockroachExternalIODisabled bool
}

func runStartSQL(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

	tenant := getTenantBinding()
	serverCfg.SQLConfig.TenantID = roachpb.MakeTenantID(tenant.tenantID)
	serverCfg.SSLCertsDir = tenantDataDir

	const clusterName = ""

	// Remove the default store, which avoids using it to set up logging.
	// Instead, we'll default to logging to stderr unless --log-dir is
	// specified. This makes sense since the standalone SQL server is
	// at the time of writing stateless and may not be provisioned with
	// suitable storage.
	serverCfg.Stores.Specs = nil

	st := serverCfg.BaseConfig.Settings

	// This value is injected in order to have something populated during startup.
	// In the initial 20.2 release of multi-tenant clusters, no version state was
	// ever populated in the version cluster setting. A value is populated during
	// the activation of 21.1. See the documentation attached to the TenantCluster
	// in migration/migrationcluster for more details on the tenant upgrade flow.
	// Note that a the value of 21.1 is populated when a tenant cluster is created
	// during 21.1 in crdb_internal.create_tenant.
	//
	// Note that the tenant will read the value in the system.settings table
	// before accepting SQL connections.
	if err := clusterversion.Initialize(
		ctx, st.Version.BinaryMinSupportedVersion(), &st.SV,
	); err != nil {
		return err
	}

	tempStorageMaxSizeBytes := int64(base.DefaultInMemTempStorageMaxSizeBytes)
	if err := diskTempStorageSizeValue.Resolve(
		&tempStorageMaxSizeBytes, memoryPercentResolver,
	); err != nil {
		return err
	}

	serverCfg.SQLConfig.TempStorageConfig = base.TempStorageConfigFromEnv(
		ctx,
		st,
		base.StoreSpec{InMemory: true},
		"", // parentDir
		tempStorageMaxSizeBytes,
	)

	initGEOS(ctx)

	sqlServer, addr, httpAddr, err := server.StartTenant(
		ctx,
		stopper,
		clusterName,
		serverCfg.BaseConfig,
		serverCfg.SQLConfig,
	)
	if err != nil {
		return err
	}

	// Start up the diagnostics reporting loop.
	// We don't do this in (*server.SQLServer).preStart() because we don't
	// want this overhead and possible interference in tests.
	if !cluster.TelemetryOptOut() {
		sqlServer.StartDiagnostics(ctx)
	}

	log.Ops.Warningf(ctx, "SQL server for tenant %s listening at %s, http at %s", serverCfg.SQLConfig.TenantID, addr, httpAddr)

	// TODO(tbg): make the other goodies in `./cockroach start` reusable, such as
	// logging to files, periodic memory output, heap and goroutine dumps, debug
	// server, graceful drain. Then use them here.

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, drainSignals...)
	select {
	case sig := <-ch:
		log.Flush()
		return errors.Newf("received signal %v", sig)
	case <-stopper.ShouldQuiesce():
		return nil
	}
}

// sqlstarter is used as part of the suspend/resume work.
//
// How this works:
// ---------------
// 1. Resumer (sqlproxy for now) will send a ZIP archive containing the CA cert,
//    tenant client certs (for SQL to KV connection), and tenant node certs (for
//    SQL server) to sqlstarter at https://IP:8081/start?tenant_id=X.
//    All connections should be encrypted.
// 2. sqlstarter extracts certs and puts them into a shared local volume
//    /cockroach/tenant-data so that certs are persisted across container
//    restarts.
// 3. sqlstarter calls execve(2) through unix.Exec to replace its own process
//    with the SQL server. Note that Kubernetes logs will still be displayed
//    properly since sqlstarter is the main entrypoint.
func getTenantBinding() *Tenant {
	options.listenAddress = "0.0.0.0:8081"
	options.cert = "/cockroach/sqlstarter-certs/sqlstarter.crt"
	options.key = "/cockroach/sqlstarter-certs/sqlstarter.key"
	options.insecure = false

	return runBindingServer()
}

// TODO(jay): Tests for this function are nice to have, but not necessary.
func runBindingServer() *Tenant {
	tenant := tryGetTenant()
	if tenant != nil {
		return tenant
	}

	h := handler{tenant: make(chan *Tenant)}

	log.Warningf(context.Background(), "listening on %s", options.listenAddress)

	// TODO(jay): Handle liveness probe and configure tenantpool to use it.
	// The liveness endpoint should be hosted on the same address and port as
	// the one for CRDB. When we start implementing this, we should support
	// restarting of stamped SQL pods by Kubernetes due to the liveness probe's
	// behavior. This can be done by checking the existence of tenant certs,
	// and verifying the tenant ID with the pod's label (or checkpoint file).
	// We will need to query the Kubernetes API during startup for the pod's
	// label.

	// Serve in HTTP mode.
	go func() {
		var err error
		http.HandleFunc("/start", h.bindingHandler)
		if options.insecure {
			err = http.ListenAndServe(options.listenAddress, nil /* handler */)
		} else {
			err = http.ListenAndServeTLS(
				options.listenAddress, options.cert, options.key, nil, /* handler */
			)
		}
		if err != nil {
			panic(err)
		}
	}()

	return <-h.tenant
}

type handler struct {
	tenant chan *Tenant
}

// startHandler is a POST handler function for the sqlstarter start endpoint.
// This expects a tenant_id in the query string and an archive file which
// contains the required tenant certs as the body. When successful, the entire
// sqlstarter process will be replaced with the standalone SQL process for
// the given tenant, and this function will never return. This is thread-safe
// and handles multiple concurrent resumers trying to resume the same SQL pod.
func (h *handler) bindingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.NotFound(w, r)
	}
	log.Warning(r.Context(), "receiving start request")

	// Extract tenant_id from query string.
	tenantIDStr := r.URL.Query().Get("tenant_id")
	if tenantIDStr == "" {
		log.Error(r.Context(), "missing tenant_id")
		http.Error(w, "missing tenant_id", http.StatusBadRequest)
	}

	// Validate tenant ID.
	// N.B. This goes into unix.Exec, so validation is necessary.
	tenantID, err := strconv.ParseUint(tenantIDStr, 10, 64)
	if err != nil {
		log.Error(r.Context(), err.Error())
		http.Error(w, "invalid tenant_id", http.StatusBadRequest)
		return
	}

	// We expect the starter archive size to be up to starterFileLimit bytes.
	buf := new(bytes.Buffer)
	if err := copyMax(buf, r.Body, starterFileLimit); err != nil {
		log.Error(r.Context(), err.Error())
		http.Error(w, "invalid starter archive", http.StatusBadRequest)
		return
	}

	contents, err := unzip(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		log.Error(r.Context(), err.Error())
		http.Error(w, "unable to read starter archive",
			http.StatusBadRequest)
		return
	}

	if err := validateStarterArchive(tenantID, contents); err != nil {
		log.Error(r.Context(), err.Error())
		http.Error(w, "invalid starter archive", http.StatusBadRequest)
		return
	}

	// Write to a shared local volume so that data is persisted across
	// container restarts. atomicWriteFile ensures that the file is created
	// or replaced atomically in the case of multiple resumers trying to
	// resume the same SQL pod by sending certs to it.
	for filename, data := range contents {
		if err := atomicWriteFile(
			fmt.Sprintf("%s/%s", tenantDataDir, filename),
			data); err != nil {
			log.Error(r.Context(), err.Error())
			http.Error(w, fmt.Sprintf("unable to write %s", filename),
				http.StatusInternalServerError)
			return
		}
	}

	// Add checkpoint marker so that we don't block on sqlstarter whenever
	// the container gets restarted.
	if err := atomicWriteFile(
		fmt.Sprintf("%s/%s", tenantDataDir, checkpointFileName),
		[]byte(tenantIDStr),
	); err != nil {
		log.Error(r.Context(), err.Error())
		http.Error(w, "unable to write checkpoint marker",
			http.StatusInternalServerError)
		return
	}

	// The first resumer that succeeds in calling this will replace the entire
	// process. When that happens while having multiple resumers in-flight,
	// we may end up with temporary files, which is fine. One improvement which
	// could be made is to limit the number of in-flight requests.
	h.tenant <- tenantFromID(tenantID)
}

// tryStartSQLProcessOrPanic tries to start the SQL process if the checkpoint
// file is present. Once attempted, it will panic if an error has occurred.
func tryGetTenant() *Tenant {
	// The file should only contain the tenant ID, so read all data to memory.
	tenantIDBytes, err := ioutil.ReadFile(
		fmt.Sprintf("%s/%s", tenantDataDir, checkpointFileName))
	if err == nil {
		tenantID, err := strconv.ParseUint(string(tenantIDBytes), 10, 64)
		if err != nil {
			// TenantID should be present if file is readable.
			panic(err)
		}
		return tenantFromID(tenantID)
	}

	if !strings.Contains(err.Error(), "no such file or directory") {
		panic(err)
	}
	return nil
}

type Tenant struct {
	tenantID uint64
}

func tenantFromID(id uint64) *Tenant {
	return &Tenant{tenantID: id}
}

// validateStarterArchive ensures that the archive contains the required files
// needed to start the SQL process:
// - ca.crt
// - tenant client certs: client-tenant.<tenant_id>.{crt,key}
// - tenant node certs: node.{crt,key}
func validateStarterArchive(tenantID uint64, contents map[string][]byte) error {
	if len(contents) != 5 {
		return errors.New("insufficient files")
	}
	for filename := range contents {
		parts := strings.Split(filename, ".")
		if len(parts) < 2 || len(parts) > 3 ||
			(parts[len(parts)-1] != "crt" && parts[len(parts)-1] != "key") {
			return errors.Newf("unrecognized file %s", filename)
		}

		if len(parts) == 2 { // CA and tenant node certs
			// N.B. This technically allows ca.key, but we'll just let it go.
			if parts[0] != "ca" && parts[0] != "node" {
				return errors.Newf("unrecognized file %s", filename)
			}
		} else { // Tenant client certs
			if parts[0] != "client-tenant" {
				return errors.Newf("unrecognized file %s", filename)
			}
			if parts[1] != strconv.FormatUint(tenantID, 10 /* base */) {
				return errors.Newf("expected client certs for tenant %d, but got %s",
					tenantID, parts[1])
			}
		}
	}
	return nil
}

// copyMax copies up to n bytes (or until an error) from src to dst.
// If n <= 0, nothing should be copied. Note that even if it returns an error
// when src has more than n bytes, the first n bytes will still be copied to
// dst, and caller should check error before proceeding.
func copyMax(dst io.Writer, src io.Reader, n int64) error {
	if n < 0 {
		return nil // Nothing should be copied
	}
	if _, err := io.CopyN(dst, src, n); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	// We copied up to n bytes into dst earlier, so check if we can read more
	// to determine if src has more than n bytes.
	nextByte := make([]byte, 1)
	nRead, err := io.ReadFull(src, nextByte)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if nRead > 0 {
		return errors.New("too much data")
	}
	return nil
}

// unzip treats the contents of the reader r up to size bytes as a zip file,
// and extracts it into an in-memory map. This only supports unzipping
// archives with top-level files.
func unzip(r io.ReaderAt, size int64) (map[string][]byte, error) {
	zipReader, err := zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}
	contents := make(map[string][]byte)
	for _, f := range zipReader.File {
		// Do not support archives with directories.
		if strings.Contains(f.Name, "/") {
			return nil, errors.New("archives with directories unsupported")
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		tmpbuf := new(bytes.Buffer)
		if _, err := io.Copy(tmpbuf, rc); err != nil {
			return nil, err
		}
		contents[f.Name] = tmpbuf.Bytes()
	}
	return contents, nil
}
