# Server Sync Monitoring

This folder contains models for monitoring whether all servers (p1, p2, p3) are in sync with each other.

## When to Use Each Approach

We provide two different ways to check server synchronization:

1. **Test** (`tests/test_servers_exact_sync.sql`):
   - Use for CI/CD pipelines and automated testing
   - Gives a simple pass/fail result
   - Blocks pipelines if servers are out of sync
   - Run with: `dbt test --models test_servers_exact_sync`

2. **Model** (`models/monitoring/server_sync_check.sql`):
   - Use for monitoring and reporting
   - Creates a table with historical sync status
   - Good for dashboards and alerting
   - Run with: `dbt run --models server_sync_check`

## Running as Separate Jobs

### Test Job

To run just the sync test as a standalone job:

```bash
# Run only the server sync test
dbt test --models test_servers_exact_sync

# Run as part of a larger test suite
dbt test --models tag:server_sync
```

### Monitoring Job

To run the monitoring model as a scheduled job:

```bash
# Run only the server sync monitoring model
dbt run --models server_sync_check

# Include with other monitoring models
dbt run --models +server_sync_check
```

### Production Workflow Example

Here's how you might set up these jobs in production:

1. **After Data Load Job**:
   ```bash
   # First run the incremental loads
   dbt run --models stg_*_deals inter_deals
   
   # Then check if servers are in sync
   dbt test --models test_servers_exact_sync
   ```

2. **Daily Monitoring Job**:
   ```bash
   # Run daily to keep track of sync status over time
   dbt run --models server_sync_check
   ```

## server_sync_check

The `server_sync_check` model verifies if all servers have exactly the same maximum timestamp.

### Logic

The check is very simple:
1. Get the maximum timestamp for each server
2. Count the number of distinct maximum timestamps
3. If count = 1, all servers are in sync
4. If count > 1, servers are out of sync

### Output Columns

- `sync_status`: Either 'IN SYNC' or 'OUT OF SYNC'
- `distinct_timestamps_count`: The number of different max timestamps found
- `checked_at`: When the check was run

## Usage

You can run this model independently to check server sync status:

```bash
dbt run --models server_sync_check
```

## Related Test

A test is also available that will fail if servers are out of sync:

```bash
dbt test --models test_servers_exact_sync
```

## Programmatic Usage

You can use the `check_servers_exact_sync` macro in other dbt models or macros:

```sql
{% if check_servers_exact_sync() %}
  -- All servers in sync, proceed with further processing
{% else %}
  -- Servers out of sync, handle appropriately
{% endif %}
``` 