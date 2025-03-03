# CockroachDB Testing Rules

## Basic Test Server Setup

```go
func TestYourFeature(t *testing.T) {
    defer leaktest.AfterTest(t)()
    defer log.Scope(t).Close(t)
    
    ctx := context.Background()
    srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
    defer srv.Stopper().Stop(ctx)
}
```

## SQL Operations

- Use `sqlutils.MakeSQLRunner` for SQL operations:
```go
tdb := sqlutils.MakeSQLRunner(sqlDB)
tdb.Exec(t, "CREATE TABLE foo (id INT)")
tdb.QueryStr(t, "SELECT * FROM foo")
```

## Accessing Server Components

Before writing code to access server components, always search for examples in
the codebase first. Here's how to find and use common components:

### Finding Examples

Search for examples using queries like:
- `serverutils.StartServer example accessing SQL server`
- `ApplicationLayer() example accessing job registry`
- `InternalExecutor example`

### Common Component Access Patterns

```go
// Get application layer
appLayer := srv.ApplicationLayer()

// Access SQL server
sqlServer := appLayer.SQLServer().(*sql.Server)

// Get internal executor
ie := appLayer.InternalExecutor().(isql.Executor)

// Access HTTP client
client, err := srv.GetAdminHTTPClient()
```

## Job System Testing

```go
// Get job registry
registry := srv.ApplicationLayer().JobRegistry().(*jobs.Registry)

// Create a job
rec := jobs.Record{
    Details:  jobspb.ImportDetails{},
    Progress: jobspb.ImportProgress{},
    Username: username.TestUserName(),
}
jobID, err := registry.CreateJobWithTxn(ctx, rec, 1, nil)
```

## Testing HTTP Endpoints

```go
// Get admin client
client, err := srv.GetAdminHTTPClient()
require.NoError(t, err)

// Make HTTP requests
var resp serverpb.HealthResponse
err = srvtestutils.GetAdminJSONProto(appLayer, "health", &resp)
```

## Testing with Multiple Nodes

```go
tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
    ServerArgs: base.TestServerArgs{
        DefaultTestTenant: base.TestControlsTenantsExplicitly,
    },
})
defer tc.Stopper().Stop(ctx)

// Access individual nodes
node0 := tc.Server(0)
node1 := tc.Server(1)
```

## Best Practices

1. Always use `defer leaktest.AfterTest(t)()` and `defer log.Scope(t).Close(t)`
2. Use `sqlutils.MakeSQLRunner` for SQL operations
3. Use `require.NoError` for error checking
4. Use `testutils.SucceedsSoon` for operations that might need retries
5. Clean up resources with `defer srv.Stopper().Stop(ctx)`
6. Use the application layer interface (`ApplicationLayer()`) to access server components
7. For tenant tests, consider using `TestControlsTenantsExplicitly`

## Common Test Server Arguments

```go
args := base.TestServerArgs{
    Insecure: true,  // For testing without TLS
    UseDatabase: "testdb",  // Set default database
    DefaultTestTenant: base.TestControlsTenantsExplicitly,  // Control tenant behavior
    Knobs: &server.TestingKnobs{
        // Add testing knobs as needed
    },
}
```
