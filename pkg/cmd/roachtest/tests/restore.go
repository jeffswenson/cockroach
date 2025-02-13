// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func registerRestoreNodeShutdown(r registry.Registry) {
	sp := restoreSpecs{
		hardware: makeHardwareSpecs(hardwareSpecs{}),
		fixture:  SmallFixture,
		timeout:  1 * time.Hour,
	}

	makeRestoreStarter := func(ctx context.Context, t test.Test, c cluster.Cluster,
		gatewayNode int, rd restoreDriver) jobStarter {
		return func(c cluster.Cluster, l *logger.Logger) (jobspb.JobID, error) {
			return rd.runDetached(ctx, "DATABASE tpce", gatewayNode)
		}
	}

	r.Add(registry.TestSpec{
		Name:                      "restore/nodeShutdown/worker",
		Owner:                     registry.OwnerDisasterRecovery,
		Cluster:                   sp.hardware.makeClusterSpecs(r),
		CompatibleClouds:          sp.fixture.CompatibleClouds,
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Leases:                    registry.MetamorphicLeases,
		Timeout:                   sp.timeout,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 3

			rd := makeRestoreDriver(ctx, t, c, sp)
			rd.prepareCluster(ctx)
			cfg := defaultNodeShutdownConfig(c, nodeToShutdown)
			cfg.restartSettings = rd.defaultClusterSettings()
			require.NoError(t,
				executeNodeShutdown(ctx, t, c, cfg,
					makeRestoreStarter(ctx, t, c, gatewayNode, rd)))
			rd.checkFingerprint(ctx)
		},
	})

	r.Add(registry.TestSpec{
		Name:                      "restore/nodeShutdown/coordinator",
		Owner:                     registry.OwnerDisasterRecovery,
		Cluster:                   sp.hardware.makeClusterSpecs(r),
		CompatibleClouds:          sp.fixture.CompatibleClouds,
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Leases:                    registry.MetamorphicLeases,
		Timeout:                   sp.timeout,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := 2
			nodeToShutdown := 2

			rd := makeRestoreDriver(ctx, t, c, sp)
			rd.prepareCluster(ctx)
			cfg := defaultNodeShutdownConfig(c, nodeToShutdown)
			cfg.restartSettings = rd.defaultClusterSettings()
			require.NoError(t,
				executeNodeShutdown(ctx, t, c, cfg,
					makeRestoreStarter(ctx, t, c, gatewayNode, rd)))
			rd.checkFingerprint(ctx)
		},
	})
}

func registerRestore(r registry.Registry) {

	durationGauge := r.PromFactory().NewGaugeVec(prometheus.GaugeOpts{Namespace: registry.
		PrometheusNameSpace, Subsystem: "restore", Name: "duration"}, []string{"test_name"})

	withPauseSpecs := restoreSpecs{
		hardware: makeHardwareSpecs(hardwareSpecs{ebsThroughput: 250 /* MB/s */}),
		fixture:  SmallFixture,
		timeout:  3 * time.Hour,
	}
	withPauseSpecs.initTestName()

	r.Add(registry.TestSpec{
		Name:                      withPauseSpecs.testName + "/pause",
		Owner:                     registry.OwnerDisasterRecovery,
		Benchmark:                 true,
		Cluster:                   withPauseSpecs.hardware.makeClusterSpecs(r),
		Timeout:                   withPauseSpecs.timeout,
		CompatibleClouds:          withPauseSpecs.fixture.CompatibleClouds,
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

			rd := makeRestoreDriver(ctx, t, c, withPauseSpecs)
			rd.prepareCluster(ctx)

			// Run the disk usage logger in the monitor to guarantee its
			// having terminated when the test ends.
			m := c.NewMonitor(ctx)
			dul := roachtestutil.NewDiskUsageLogger(t, c)
			m.Go(dul.Runner)

			jobIDCh := make(chan jobspb.JobID)
			jobCompleteCh := make(chan struct{}, 1)

			pauseAtProgress := []float64{0.2, 0.45, 0.7}
			for i := range pauseAtProgress {
				// Add up to 10% to the pause point.
				pauseAtProgress[i] = pauseAtProgress[i] + float64(rand.Intn(10))/100
			}
			pauseIndex := 0
			// Spin up go routine which pauses and resumes the Restore job three times.
			m.Go(func(ctx context.Context) error {
				// Wait until the restore job has been created.
				conn, err := c.ConnE(ctx, t.L(), c.Node(1)[0])
				require.NoError(t, err)
				sql := sqlutils.MakeSQLRunner(conn)

				// The job should be created fairly quickly once the roachtest starts.
				done := ctx.Done()
				jobID := <-jobIDCh

				jobProgressTick := time.NewTicker(time.Second * 5)
				defer jobProgressTick.Stop()
				for {
					if pauseIndex == len(pauseAtProgress) {
						t.L().Printf("RESTORE job was paused a maximum number of times; allowing the job to complete")
						return nil
					}
					select {
					case <-done:
						return ctx.Err()
					case <-jobCompleteCh:
						return nil
					case <-jobProgressTick.C:
						var fraction gosql.NullFloat64
						sql.QueryRow(t, `SELECT fraction_completed FROM [SHOW JOB $1]`,
							jobID).Scan(&fraction)
						t.L().Printf("RESTORE Progress %.2f", fraction)
						if !fraction.Valid || fraction.Float64 < pauseAtProgress[pauseIndex] {
							continue
						}
						t.L().Printf("pausing RESTORE job since progress is greater than %.2f", pauseAtProgress[pauseIndex])
						// Pause the job and wait for it to transition to a paused state.
						_, err := conn.Query(`PAUSE JOB $1`, jobID)
						if err != nil {
							// The pause job request should not fail unless the job has already succeeded,
							// in which case, the test should gracefully succeed.
							var status string
							sql.QueryRow(t, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status)
							if status == "succeeded" {
								return nil
							}
						}
						require.NoError(t, err)
						testutils.SucceedsWithin(t, func() error {
							var status string
							sql.QueryRow(t, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status)
							if status != "paused" {
								return errors.Newf("expected status `paused` but found %s", status)
							}
							t.L().Printf("paused RESTORE job")
							pauseIndex++
							return nil
						}, 2*time.Minute)

						t.L().Printf("resuming RESTORE job")
						sql.Exec(t, `RESUME JOB $1`, jobID)
					}
				}
			})

			m.Go(func(ctx context.Context) error {
				defer dul.Done()
				defer close(jobCompleteCh)
				defer close(jobIDCh)
				t.Status(`running restore`)
				metricCollector := rd.initRestorePerfMetrics(ctx, durationGauge)
				jobID, err := rd.runDetached(ctx, "DATABASE tpce", 1)
				require.NoError(t, err)
				jobIDCh <- jobID

				// Wait for the job to succeed.
				succeededJobTick := time.NewTicker(time.Minute * 1)
				defer succeededJobTick.Stop()
				done := ctx.Done()
				conn, err := c.ConnE(ctx, t.L(), c.Node(1)[0])
				require.NoError(t, err)
				var isJobComplete bool
				for {
					if isJobComplete {
						succeededJobTick.Stop()
						jobCompleteCh <- struct{}{}
						break
					}

					select {
					case <-done:
						return ctx.Err()
					case <-jobCompleteCh:
						return nil
					case <-succeededJobTick.C:
						var status string
						err := conn.QueryRow(`SELECT status FROM [SHOW JOBS] WHERE job_type = 'RESTORE'`).Scan(&status)
						require.NoError(t, err)
						if status == string(jobs.StatusSucceeded) {
							isJobComplete = true
						} else if status == string(jobs.StatusFailed) || status == string(jobs.StatusCanceled) {
							t.Fatalf("job unexpectedly found in %s state", status)
						}
					}
				}
				metricCollector()
				rd.checkFingerprint(ctx)
				return nil
			})
			m.Wait()
			// All failures from the above go routines surface via a t.Fatal() within
			// the m.Wait( ) call above; therefore, at this point, the restore job
			// should have succeeded. This final check ensures this test is actually
			// doing its job: causing the restore job to pause at least once.
			require.NotEqual(t, 0, pauseIndex, "the job should have paused at least once")
		},

		// TODO(msbutler): to test the correctness of checkpointing, we should
		// restore the same fixture without pausing it and fingerprint both restored
		// databases.
	})

	for _, sp := range []restoreSpecs{
		{
			hardware:         makeHardwareSpecs(hardwareSpecs{}),
			compatibleClouds: registry.Clouds(spec.Local),
			timeout:          15 * time.Minute,
			fixture:          TinyFixture,
			useFullBackup:    false,
			suites:           registry.Suites(registry.Nightly),
		},
		{
			hardware:         makeHardwareSpecs(hardwareSpecs{nodes: 4, ebsThroughput: 250 /* MB/s */}),
			fixture:          SmallFixture,
			compatibleClouds: SmallFixture.CompatibleClouds,
			timeout:          1 * time.Hour,
			suites:           registry.Suites(registry.Nightly),
			useFullBackup:    true,
		},
		{
			// Benchmarks if per node throughput remains constant if the number of
			// nodes doubles relative to default.
			hardware:         makeHardwareSpecs(hardwareSpecs{nodes: 8, ebsThroughput: 250 /* MB/s */}),
			fixture:          SmallFixture,
			compatibleClouds: SmallFixture.CompatibleClouds,
			timeout:          1 * time.Hour,
			suites:           registry.Suites(registry.Nightly),
			useFullBackup:    true,
		},
		{
			// Benchmarks per node throughput for restores with an incremental chain.
			hardware:         makeHardwareSpecs(hardwareSpecs{nodes: 4, ebsThroughput: 250 /* MB/s */}),
			fixture:          SmallFixture,
			compatibleClouds: SmallFixture.CompatibleClouds,
			suites:           registry.Suites(registry.Nightly),
			timeout:          2 * time.Hour,
		},
		{
			// The weekly 20T Restore test. Restore on to three maximum density
			// nodes. This is intended to stress large stores during restore.
			hardware:         makeHardwareSpecs(hardwareSpecs{nodes: 3, cpus: 32, volumeSize: 8000, ebsThroughput: 1000 /* MB/s */}),
			timeout:          24 * time.Hour,
			fixture:          LargeFixture,
			compatibleClouds: LargeFixture.CompatibleClouds,
			suites:           registry.Suites(registry.Weekly),
		},
		{
			// The weekly 2T Restore test. Restore on to three minimum sized nodes.
			// This is intended to stress restores on small nodes.
			hardware:         makeHardwareSpecs(hardwareSpecs{nodes: 3, cpus: 4, volumeSize: 800, ebsThroughput: 250 /* MB/s */}),
			compatibleClouds: MediumFixture.CompatibleClouds,
			fixture:          MediumFixture,
			timeout:          24 * time.Hour,
			suites:           registry.Suites(registry.Weekly),
		},
	} {
		sp := sp
		sp.initTestName()

		if sp.fixture.Name == "" {
			panic(sp.testName + " has no fixture name")
		}
		if !sp.fixture.CompatibleClouds.IsInitialized() {
			panic(sp.fixture.Name + " has no compatible clouds")
		}
		if !sp.suites.IsInitialized() {
			panic(sp.testName + " has no suites")
		}

		r.Add(registry.TestSpec{
			Name:      sp.testName,
			Owner:     registry.OwnerDisasterRecovery,
			Benchmark: true,
			Cluster:   sp.hardware.makeClusterSpecs(r),
			Timeout:   sp.timeout,
			// These tests measure performance. To ensure consistent perf,
			// disable metamorphic encryption.
			EncryptionSupport:         registry.EncryptionAlwaysDisabled,
			CompatibleClouds:          sp.compatibleClouds,
			Suites:                    sp.suites,
			TestSelectionOptOutSuites: sp.suites,
			Skip:                      sp.skip,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				rd := makeRestoreDriver(ctx, t, c, sp)
				rd.prepareCluster(ctx)

				// Run the disk usage logger in the monitor to guarantee its
				// having terminated when the test ends.
				m := c.NewMonitor(ctx)
				dul := roachtestutil.NewDiskUsageLogger(t, c)
				m.Go(dul.Runner)
				m.Go(func(ctx context.Context) error {
					defer dul.Done()
					t.Status(`running setup statements`)
					db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
					if err != nil {
						return errors.Wrapf(err, "failure to run setup statements")
					}
					defer db.Close()
					// Run set-up SQL statements. In particular, enable collecting CPU
					// profiles automatically if CPU usage is high. Historically, we
					// observed CPU going as high as 100%, e.g. see issue #111160.
					// TODO(pavelkalinnikov): enable CPU profiling in all roachtests.
					for _, stmt := range append(sp.setUpStmts,
						"SET CLUSTER SETTING server.cpu_profile.duration = '2s'",
						"SET CLUSTER SETTING server.cpu_profile.cpu_usage_combined_threshold = 80",
					) {
						_, err := db.Exec(stmt)
						if err != nil {
							return errors.Wrapf(err, "error executing setup stmt [%s]", stmt)
						}
					}

					t.Status(`running restore`)
					metricCollector := rd.initRestorePerfMetrics(ctx, durationGauge)
					if err := rd.run(ctx, "DATABASE tpcc"); err != nil {
						return err
					}
					metricCollector()
					rd.checkFingerprint(ctx)
					return nil
				})
				m.Wait()
			},
		})
	}
}

var defaultHardware = hardwareSpecs{
	cpus:       8,
	nodes:      4,
	volumeSize: 1000,
}

// hardwareSpecs define the cluster setup for a restore roachtest. These values
// should not get updated as the test runs.
type hardwareSpecs struct {

	// cpus is the per node cpu count.
	cpus int

	// nodes is the number of crdb nodes in the restore.
	nodes int

	// addWorkloadNode is true if workload node should also get spun up
	workloadNode bool

	// volumeSize indicates the size of per node block storage (pd-ssd for gcs,
	// ebs for aws). If zero, local ssd's are used.
	volumeSize int
	// ebsThroughput is the min provisioned throughput of the EBS volume, in MB/s.
	// TODO(pavelkalinnikov): support provisioning throughput not only on EBS.
	ebsThroughput int

	// mem is the memory per cpu.
	mem spec.MemPerCPU

	// Availability zones to use. (Values are cloud-provider-specific.)
	// If unset, the first of the default availability zones for the provider will be used.
	zones []string
}

func (hw hardwareSpecs) makeClusterSpecs(r registry.Registry) spec.ClusterSpec {
	clusterOpts := make([]spec.Option, 0)
	clusterOpts = append(clusterOpts, spec.CPU(hw.cpus))
	if hw.volumeSize != 0 {
		clusterOpts = append(clusterOpts, spec.VolumeSize(hw.volumeSize))
	}
	if hw.mem != spec.Auto {
		clusterOpts = append(clusterOpts, spec.Mem(hw.mem))
	}
	addWorkloadNode := 0
	if hw.workloadNode {
		addWorkloadNode++
		clusterOpts = append(clusterOpts, spec.WorkloadNodeCount(1))
	}
	if len(hw.zones) > 0 {
		// Each test is set up to run on one specific cloud, so it's ok that the
		// zones will only make sense for one of them.
		// TODO(radu): clean this up.
		clusterOpts = append(clusterOpts, spec.GCEZones(strings.Join(hw.zones, ",")))
		clusterOpts = append(clusterOpts, spec.AWSZones(strings.Join(hw.zones, ",")))
		clusterOpts = append(clusterOpts, spec.Geo())
	}
	if hw.ebsThroughput != 0 {
		clusterOpts = append(clusterOpts, spec.AWSVolumeThroughput(hw.ebsThroughput))
	}
	s := r.MakeClusterSpec(hw.nodes+addWorkloadNode, clusterOpts...)

	return s
}

// String prints the hardware specs. If verbose==true, verbose specs are printed.
func (hw hardwareSpecs) String() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("/nodes=%d", hw.nodes))
	builder.WriteString(fmt.Sprintf("/cpus=%d", hw.cpus))
	if hw.mem != spec.Auto {
		builder.WriteString(fmt.Sprintf("/%smem", hw.mem))
	}
	if len(hw.zones) > 0 {
		builder.WriteString(fmt.Sprintf("/zones=%s", strings.Join(hw.zones, ",")))
	}
	return builder.String()
}

func (hw hardwareSpecs) getWorkloadNode() int {
	if !hw.workloadNode {
		panic(`this test does not have a workload node`)
	}
	return hw.nodes + 1
}

func (hw hardwareSpecs) getCRDBNodes() option.NodeListOption {
	nodes := make(option.NodeListOption, hw.nodes)
	for i := range nodes {
		nodes[i] = i + 1
	}
	return nodes
}

// makeHardwareSpecs instantiates hardware specs for a restore roachtest.
// Unless the caller provides any explicit specs, the default specs are used.
func makeHardwareSpecs(override hardwareSpecs) hardwareSpecs {
	specs := defaultHardware
	if override.cpus != 0 {
		specs.cpus = override.cpus
	}
	if override.nodes != 0 {
		specs.nodes = override.nodes
	}
	if override.mem != spec.Auto {
		specs.mem = override.mem
	}
	if override.volumeSize != 0 {
		specs.volumeSize = override.volumeSize
	}
	if override.ebsThroughput != 0 {
		specs.ebsThroughput = override.ebsThroughput
	}
	specs.zones = override.zones
	specs.workloadNode = override.workloadNode
	return specs
}

var defaultRestoringBackupSpecs = backupSpecs{
	// TODO(msbutler): write a script that automatically finds the latest versioned fixture.
	cloud:   spec.GCE,
	fixture: SmallFixture,
	// TODO(jeffswenson): configure the wrokload
	// workload:          tpccWorkload{tpccWarehouseCount: 1000},
}

// backupSpecs define the backup that will get restored. These values should not
// get updated during the test.
type backupSpecs struct {
	// version specifies the crdb version the backup was taken on.
	version string

	// cloud is the cloud storage provider the backup is stored on.
	cloud spec.Cloud

	// allowLocal is true if the test should be allowed to run
	// locally. We don't set this by default to avoid someone
	// trying to run the very large roachtests locally.
	allowLocal bool

	// fixture is the fixture that should be restored.
	fixture TpccFixture

	// useFullBackup is true if the restore should use the full backup instead of
	// the icremental backup.
	useFullBackup bool

	// workload defines the backed up workload.
	workload backupWorkload
}

func (bs backupSpecs) CloudIsCompatible(cloud spec.Cloud) error {
	if cloud == spec.Local && bs.allowLocal {
		return nil
	}
	if cloud != bs.cloud {
		// For now, only run the test on the cloud provider
		// that also stores the backup.
		return errors.Newf("test configured to run on %q (not %q)", bs.cloud, cloud)
	}
	return nil
}

func (bs backupSpecs) CompatibleClouds() registry.CloudSet {
	r := registry.Clouds(bs.cloud)
	if bs.allowLocal {
		r = registry.Clouds(bs.cloud, spec.Local)
	}
	return r
}

func (bs backupSpecs) storagePrefix() string {
	if bs.cloud == spec.AWS {
		return "s3"
	}
	return "gs"
}

// getBackupEndTimeCmd returns a sql cmd that will return a system time that is
// equal to the end time of the bs.backupsIncluded'th backup in the target
// backup chain.
func getBackupEndTimeCmd(uri url.URL, useFullBackup bool) string {
	if useFullBackup {
		return fmt.Sprintf(
			`SELECT min(end_time) FROM [SHOW BACKUP FROM LATEST IN '%s']`,
			uri.String())
	} else {
		return fmt.Sprintf(
			`SELECT max(end_time) FROM [SHOW BACKUP FROM LATEST IN '%s']`,
			uri.String())
	}
}

func makeBackupSpecs(override backupSpecs, specs backupSpecs) backupSpecs {
	if override.cloud.IsSet() {
		specs.cloud = override.cloud
	}
	if override.version != "" {
		specs.version = override.version
	}

	if override.fixture.Name != "" {
		specs.fixture = override.fixture
	}

	if override.useFullBackup {
		specs.useFullBackup = override.useFullBackup
	}

	if override.workload != nil {
		specs.workload = override.workload
	}
	return specs
}

// makeRestoringBackupSpecs initializes the default restoring backup specs. The caller can override
// any of the default backup specs by passing any non-nil params.
func makeRestoringBackupSpecs(override backupSpecs) backupSpecs {
	return makeBackupSpecs(override, defaultRestoringBackupSpecs)
}

type backupWorkload interface {
	fixtureDir() string
	String() string

	// DatabaseName specifies the name of the database the workload will operate on.
	DatabaseName() string

	// init loads the cluster with the workload's schema and initial data.
	init(ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs)

	// run begins a workload that runs indefinitely until the passed context
	// is cancelled.
	run(ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs) error
}

type tpceRestore struct {
	customers int
	spec      *tpceSpec
}

func (tpce tpceRestore) getSpec(
	ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs,
) *tpceSpec {
	if tpce.spec != nil {
		return tpce.spec
	}
	tpceSpec, err := initTPCESpec(ctx, t.L(), c)
	require.NoError(t, err)
	return tpceSpec
}

func (tpce tpceRestore) init(
	ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs,
) {
	spec := tpce.getSpec(ctx, t, c, sp)
	spec.init(ctx, t, c, tpceCmdOptions{
		customers:      tpce.customers,
		racks:          sp.nodes,
		connectionOpts: tpceConnectionOpts{fixtureBucket: defaultFixtureBucket},
	})
}

func (tpce tpceRestore) run(
	ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs,
) error {
	spec := tpce.getSpec(ctx, t, c, sp)
	details, err := spec.run(ctx, t, c, tpceCmdOptions{
		// Set the duration to be a week to ensure the workload never exits early.
		duration:       time.Hour * 7 * 24,
		customers:      tpce.customers,
		racks:          sp.nodes,
		threads:        sp.cpus * sp.nodes,
		connectionOpts: tpceConnectionOpts{fixtureBucket: defaultFixtureBucket},
	})
	out := details.Output(true)
	t.L().Printf("TPCE run details: \n%s\n", out)
	t.L().Printf("TPCE run error: %s\n", err)
	return err
}

func (tpce tpceRestore) fixtureDir() string {
	return fmt.Sprintf(`tpc-e/customers=%d`, tpce.customers)
}

func (tpce tpceRestore) String() string {
	var builder strings.Builder
	builder.WriteString("tpce/")
	switch tpce.customers {
	case 1000:
		builder.WriteString("15GB")
	case 5000:
		builder.WriteString("80GB")
	case 25000:
		builder.WriteString("400GB")
	case 500000:
		builder.WriteString("8TB")
	case 2000000:
		builder.WriteString("32TB")
	default:
		panic("tpce customer count not recognized")
	}
	return builder.String()
}

func (tpce tpceRestore) DatabaseName() string {
	return "tpce"
}

type tpccRestoreOptions struct {
	warehouses     int
	workers        int
	maxOps         int
	maxRate        int
	waitFraction   float64
	queryTraceFile string
	seed           uint64
	fakeTime       uint32
}

type tpccRestore struct {
	opts tpccRestoreOptions
}

func (tpcc tpccRestore) init(
	ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs,
) {
	crdbNodes := sp.getCRDBNodes()
	cmd := roachtestutil.NewCommand(`./cockroach workload init tpcc`).
		MaybeFlag(tpcc.opts.warehouses > 0, "warehouses", tpcc.opts.warehouses).
		MaybeFlag(tpcc.opts.seed != 0, "seed", tpcc.opts.seed).
		MaybeFlag(tpcc.opts.fakeTime != 0, "fake-time", tpcc.opts.fakeTime).
		Arg("{pgurl:%d-%d}", crdbNodes[0], crdbNodes[len(crdbNodes)-1])
	c.Run(ctx, option.WithNodes([]int{sp.getWorkloadNode()}), cmd.String())
}

func (tpcc tpccRestore) run(
	ctx context.Context, t test.Test, c cluster.Cluster, sp hardwareSpecs,
) error {
	crdbNodes := sp.getCRDBNodes()
	cmd := roachtestutil.NewCommand(`./cockroach workload run tpcc`).
		MaybeFlag(tpcc.opts.workers > 0, "workers", tpcc.opts.workers).
		MaybeFlag(tpcc.opts.waitFraction != 1, "wait", tpcc.opts.waitFraction).
		MaybeFlag(tpcc.opts.maxOps != 0, "max-ops", tpcc.opts.maxOps).
		MaybeFlag(tpcc.opts.maxRate != 0, "max-rate", tpcc.opts.maxRate).
		MaybeFlag(tpcc.opts.seed != 0, "seed", tpcc.opts.seed).
		MaybeFlag(tpcc.opts.fakeTime != 0, "fake-time", tpcc.opts.fakeTime).
		MaybeFlag(tpcc.opts.queryTraceFile != "", "query-trace-file", tpcc.opts.queryTraceFile).
		Arg("{pgurl:%d-%d}", crdbNodes[0], crdbNodes[len(crdbNodes)-1])
	return c.RunE(ctx, option.WithNodes([]int{sp.getWorkloadNode()}), cmd.String())
}

func (tpcc tpccRestore) fixtureDir() string {
	return fmt.Sprintf("tpc-c/warehouses=%d", tpcc.opts.warehouses)
}

func (tpcc tpccRestore) String() string {
	var builder strings.Builder
	builder.WriteString("tpcc/")
	switch tpcc.opts.warehouses {
	case 10:
		builder.WriteString("150MB")
	case 5000:
		builder.WriteString("350GB")
	case 150000:
		builder.WriteString("8TB")
	default:
		panic(fmt.Sprintf("tpcc warehouse %d count not recognized", tpcc.opts.warehouses))
	}
	return builder.String()
}

func (tpcc tpccRestore) DatabaseName() string {
	return "tpcc"
}

// restoreSpecs define input parameters to a restore roachtest set during
// registration. They should not be modified within test_spec.run(), as they are shared
// across driver runs.
type restoreSpecs struct {
	hardware hardwareSpecs

	fixture TpccFixture

	compatibleClouds registry.CloudSet

	useFullBackup bool

	timeout time.Duration
	suites  registry.SuiteSet

	workload backupWorkload

	setUpStmts []string

	// extraArgs are passed to the cockroach binary at startup.
	extraArgs []string

	// skip, if non-empty, skips the test with the given reason.
	skip string

	// testname is set automatically.
	testName string
}

func (sp *restoreSpecs) initTestName() {
	sp.testName = sp.computeName()
}

func (sp *restoreSpecs) computeName() string {
	return "restore/" + sp.String() + sp.hardware.String()
}

func (sp *restoreSpecs) String() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s", sp.fixture.Name)

	// Annotate the name with the number of incremental layers we are restoring if
	// it differs from the default.
	if !sp.useFullBackup {
		fmt.Fprintf(&builder, "/inc-count=%d", sp.fixture.IncrementalChainLength)
	} else {
		fmt.Fprintf(&builder, "/full-backup")
	}

	return builder.String()
}

type restoreDriver struct {
	sp restoreSpecs

	t test.Test
	c cluster.Cluster

	fixtureURI url.URL

	// aost defines the "As Of System Time" used within the restore. Because this
	// gets computed during test execution, it is stored in the restoreDriver
	// rather than the restoreSpecs.
	aost string

	rng *rand.Rand
}

func makeRestoreDriver(
	ctx context.Context, t test.Test, c cluster.Cluster, sp restoreSpecs,
) restoreDriver {
	rng, seed := randutil.NewPseudoRand()
	t.L().Printf(`Random Seed is %d`, seed)

	registry := newFixtureRegistry(ctx, t, c)

	fixture, err := registry.GetLatest(ctx, sp.fixture.Name)
	require.NoError(t, err)

	t.L().Printf("Using fixture %s", fixture.DataPath)

	return restoreDriver{
		t:          t,
		c:          c,
		fixtureURI: registry.URI(fixture.DataPath),
		sp:         sp,
		rng:        rng,
	}
}

func (rd *restoreDriver) defaultClusterSettings() []install.ClusterSettingOption {
	return []install.ClusterSettingOption{
		install.SecureOption(false),
	}
}

func (rd *restoreDriver) roachprodOpts() option.StartOpts {
	opts := option.NewStartOpts(option.NoBackupSchedule)
	opts.RoachprodOpts.ExtraArgs = append(opts.RoachprodOpts.ExtraArgs, rd.sp.extraArgs...)
	return opts
}

func (rd *restoreDriver) prepareCluster(ctx context.Context) {
	rd.c.Start(ctx, rd.t.L(),
		rd.roachprodOpts(),
		install.MakeClusterSettings(rd.defaultClusterSettings()...),
		rd.sp.hardware.getCRDBNodes())
	rd.getAOST(ctx)
}

// getAOST gets the AOST to use in the restore cmd.
func (rd *restoreDriver) getAOST(ctx context.Context) {
	var aost string
	conn := rd.c.Conn(ctx, rd.t.L(), 1)
	defer conn.Close()

	getAostCmd := getBackupEndTimeCmd(rd.fixtureURI, rd.sp.useFullBackup)
	err := conn.QueryRowContext(ctx, getAostCmd).Scan(&aost)
	require.NoError(rd.t, err, fmt.Sprintf("aost cmd failed: %s", getAostCmd))
	rd.aost = aost
}

func (rd *restoreDriver) restoreCmd(target, opts string) string {
	var aostSubCmd string
	if rd.aost != "" {
		aostSubCmd = fmt.Sprintf("AS OF SYSTEM TIME '%s'", rd.aost)
	}
	query := fmt.Sprintf(`RESTORE %s FROM LATEST IN '%s' %s %s`, target,
		rd.fixtureURI.String(), aostSubCmd, opts)
	rd.t.L().Printf("Running restore cmd: %s", query)
	return query
}

// run executes the restore, where target injects a restore target into the restore command.
// Examples:
// - "DATABASE tpce" will execute a database restore on the tpce cluster.
// - "" will execute a cluster restore.
func (rd *restoreDriver) run(ctx context.Context, target string) error {
	conn, err := rd.c.ConnE(ctx, rd.t.L(), 1)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to node 1; running restore")
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, rd.restoreCmd(target, "WITH unsafe_restore_incompatible_version"))
	return err
}

func (rd *restoreDriver) runDetached(
	ctx context.Context, target string, node int,
) (jobspb.JobID, error) {
	db, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(node)[0])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to connect to node %d; running restore detached", node)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, rd.restoreCmd(target,
		"WITH DETACHED, unsafe_restore_incompatible_version")); err != nil {
		return 0, err
	}
	var jobID jobspb.JobID
	if err := db.QueryRow(`SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'RESTORE'`).Scan(&jobID); err != nil {
		return 0, err
	}
	return jobID, nil
}

// initRestorePerfMetrics returns a function that will collect restore throughput at the end of
// the test.
//
// TODO(msbutler): only export metrics to test-eng prometheus server once it begins scraping
// nightly roachtest runs.
func (rd *restoreDriver) initRestorePerfMetrics(
	ctx context.Context, durationGauge *prometheus.GaugeVec,
) func() {
	dut, err := roachtestutil.NewDiskUsageTracker(rd.c, rd.t.L())
	require.NoError(rd.t, err)
	startTime := timeutil.Now()
	startDu := dut.GetDiskUsage(ctx, rd.c.All())

	return func() {
		promLabel := registry.PromSub(strings.Replace(rd.sp.testName, "restore/", "", 1)) + "_seconds"
		testDuration := timeutil.Since(startTime).Seconds()
		durationGauge.WithLabelValues(promLabel).Set(testDuration)

		// compute throughput as MB / node / second.
		du := dut.GetDiskUsage(ctx, rd.c.All())
		throughput := float64(du-startDu) / (float64(rd.sp.hardware.nodes) * testDuration)
		rd.t.L().Printf("Usage %d , Nodes %d , Duration %f\n; Throughput: %f mb / node / second",
			du,
			rd.sp.hardware.nodes,
			testDuration,
			throughput)
		exportToRoachperf(ctx, rd.t, rd.c, rd.sp.testName, int64(throughput))
	}
}

// checkFingerprint runs a stripped fingerprint on all user tables in the cluster if the restore
// spec has a nonzero fingerprint.
func (rd *restoreDriver) checkFingerprint(ctx context.Context) {
	// TODO(jeffswenson): retrieve fingerprint from fixture

	// if rd.sp.fingerprint == 0 {
	// 	rd.t.L().Printf("Fingerprint not found in specs. Skipping fingerprint check.")
	// 	return
	// }

	// conn, err := rd.c.ConnE(ctx, rd.t.L(), rd.c.Node(1)[0])
	// require.NoError(rd.t, err)
	// defer conn.Close()
	// sql := sqlutils.MakeSQLRunner(conn)

	// var minUserTableID, maxUserTableID uint32
	// sql.QueryRow(rd.t, `SELECT min(id), max(id) FROM system.namespace WHERE "parentID" >1`).Scan(
	// 	&minUserTableID, &maxUserTableID)

	// codec := keys.MakeSQLCodec(roachpb.SystemTenantID)
	// startKey := codec.TablePrefix(minUserTableID)
	// endkey := codec.TablePrefix(maxUserTableID).PrefixEnd()

	// startTime := timeutil.Now()
	// var fingerprint int
	// sql.QueryRow(rd.t, `SELECT * FROM crdb_internal.fingerprint(ARRAY[$1::BYTES, $2::BYTES],true)`,
	// 	startKey, endkey).Scan(&fingerprint)
	// rd.t.L().Printf("Fingerprint is %d. Took %.2f minutes", fingerprint,
	// 	timeutil.Since(startTime).Minutes())
	// require.Equal(rd.t, rd.sp.fingerprint, fingerprint, "user table fingerprint mismatch")
}

// exportToRoachperf exports a single perf metric for the given test to roachperf.
func exportToRoachperf(
	ctx context.Context, t test.Test, c cluster.Cluster, testName string, metric int64,
) {

	exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)

	// The easiest way to record a precise metric for roachperf is to caste it as a duration,
	// in seconds in the histogram's upper bound.
	reg := histogram.NewRegistryWithExporter(time.Duration(metric)*time.Second, histogram.MockWorkloadName, exporter)

	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)

	exporter.Init(&writer)
	defer roachtestutil.CloseExporter(ctx, exporter, t, c, bytesBuf, c.Node(1), "")
	var err error
	// Ensure the histogram contains the name of the roachtest
	reg.GetHandle().Get(testName)

	// Serialize the histogram into the buffer
	reg.Tick(func(tick histogram.Tick) {
		err = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
	})

	if err != nil {
		return
	}
}

// verifyMetrics loops, retrieving the timeseries metrics specified in m every
// 10s and verifying that the most recent value is less that the limit
// specified in m. This is particularly useful for verifying that a counter
// metric does not exceed some threshold during a test. For example, the
// restore and import tests verify that the range merge queue is inactive.
func verifyMetrics(
	ctx context.Context, t test.Test, c cluster.Cluster, m map[string]float64,
) error {
	const sample = 10 * time.Second
	// Query needed information over the timespan of the query.
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(1))
	if err != nil {
		return err
	}
	url := "https://" + adminUIAddrs[0] + "/ts/query"

	request := tspb.TimeSeriesQueryRequest{
		// Ask for one minute intervals. We can't just ask for the whole hour
		// because the time series query system does not support downsampling
		// offsets.
		SampleNanos: sample.Nanoseconds(),
	}
	for name := range m {
		request.Queries = append(request.Queries, tspb.Query{
			Name:             name,
			Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
			SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
		})
	}

	ticker := time.NewTicker(sample)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		now := timeutil.Now()
		request.StartNanos = now.Add(-sample * 3).UnixNano()
		request.EndNanos = now.UnixNano()

		var response tspb.TimeSeriesQueryResponse
		if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
			return err
		}

		for i := range request.Queries {
			name := request.Queries[i].Name
			data := response.Results[i].Datapoints
			n := len(data)
			if n == 0 {
				continue
			}
			limit := m[name]
			value := data[n-1].Value
			if value >= limit {
				return fmt.Errorf("%s: %.1f >= %.1f @ %d", name, value, limit, data[n-1].TimestampNanos)
			}
		}
	}
}

// TODO(peter): silence unused warning.
var _ = verifyMetrics
