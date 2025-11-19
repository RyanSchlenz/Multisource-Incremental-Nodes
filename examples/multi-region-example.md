# Example Configuration: Multi-Region Customer Fact Table

## Scenario
Consolidate customer data from US, EU, and APAC regional databases into a single fact table with automatic deduplication.

## Sources
- CUSTOMER_US: 10M customers, updated daily
- CUSTOMER_EU: 5M customers, updated daily  
- CUSTOMER_APAC: 3M customers, updated daily

## Configuration

### Node Properties
```
Storage Location: ANALYTICS_DB.FACT
Node Type: MultiSource Incremental Fact
Description: Consolidated customer fact table from all regions
Deploy Enabled: true
```

### Multi Source Options
```
Multi Source: true
Multi Source Strategy: UNION ALL
```

### Incremental Options
```
Enable Incremental Loading: true
Initial Full Load Mode: false (set to true for first run only)
Load Date Column: LAST_MODIFIED_DATE
Lookback Days: 1
```

### Business Keys
```
Business Key Columns: CUSTOMER_ID
```

### System Columns
```
LOAD_TIMESTAMP: Enabled (automatic)
```

### Additional Options
```
Enable Tests: true
Pre-SQL: (empty)
Post-SQL: 
  -- Update metadata table
  INSERT INTO ANALYTICS_DB.META.LOAD_HISTORY 
  VALUES ('CUSTOMER_FACT', CURRENT_TIMESTAMP(), (SELECT COUNT(*) FROM ANALYTICS_DB.FACT.CUSTOMER_FACT));
```

## Column Mapping

Ensure all source tables have matching columns:

| Column Name | Data Type | Business Key | Load Date |
|-------------|-----------|--------------|-----------|
| CUSTOMER_ID | NUMBER(38,0) | Yes | No |
| FIRST_NAME | VARCHAR(100) | No | No |
| LAST_NAME | VARCHAR(100) | No | No |
| EMAIL | VARCHAR(200) | No | No |
| PHONE | VARCHAR(50) | No | No |
| ADDRESS_LINE1 | VARCHAR(200) | No | No |
| CITY | VARCHAR(100) | No | No |
| STATE | VARCHAR(50) | No | No |
| POSTAL_CODE | VARCHAR(20) | No | No |
| COUNTRY_CODE | VARCHAR(3) | No | No |
| ACCOUNT_STATUS | VARCHAR(20) | No | No |
| CREATED_DATE | TIMESTAMP | No | No |
| LAST_MODIFIED_DATE | TIMESTAMP | No | Yes |
| SOURCE_REGION | VARCHAR(10) | No | No |

## Expected Behavior

### First Run (Initial Full Load Mode = true)
- Loads all 18M customers from all three regions
- Deduplicates within each source (keeps latest per CUSTOMER_ID)
- Deduplicates across sources (keeps latest per CUSTOMER_ID across all regions)
- Execution time: ~5-10 minutes depending on cluster size

### Subsequent Runs (Initial Full Load Mode = false, Lookback Days = 1)
- Loads only customers modified in last 1 day (~50K per day across all regions)
- Applies same deduplication logic
- MERGEs into target (UPDATEs existing, INSERTs new)
- Execution time: ~30-60 seconds

## Performance Metrics

### Before (Separate Nodes + Manual UNION)
- Nodes: 4 (3 incremental + 1 union)
- Total execution time: 8-12 minutes
- Maintenance overhead: High (4 nodes to configure)
- Deduplication: Manual SQL in downstream node

### After (MultiSource Incremental Load)
- Nodes: 1
- Total execution time: 30-60 seconds (incremental)
- Maintenance overhead: Low (single node configuration)
- Deduplication: Automatic

## Data Quality Tests

```yaml
tests:
  - name: no_null_customer_ids
    type: not_null
    column: CUSTOMER_ID
    
  - name: unique_customer_ids
    type: unique
    column: CUSTOMER_ID
    
  - name: valid_email_format
    type: custom
    sql: "SELECT COUNT(*) FROM {{ ref('CUSTOMER_FACT') }} WHERE EMAIL NOT LIKE '%@%.%'"
    expected: 0
    
  - name: reasonable_row_count
    type: custom
    sql: "SELECT COUNT(*) FROM {{ ref('CUSTOMER_FACT') }}"
    operator: ">"
    expected: 15000000
```

## Troubleshooting

### Issue: Duplicates Found
**Symptom:** Same CUSTOMER_ID appears multiple times in target table
**Solution:** 
- Verify CUSTOMER_ID is set as Business Key Column
- Check that LAST_MODIFIED_DATE has valid timestamps in all sources
- Confirm all sources have CUSTOMER_ID populated

### Issue: Missing Recent Updates
**Symptom:** Changes from today not appearing in target
**Solution:**
- Verify Lookback Days is set to 1 (or higher)
- Check LAST_MODIFIED_DATE is being updated in source systems
- Confirm Initial Full Load Mode is set to false after first run

### Issue: Slow Execution
**Symptom:** Incremental runs taking longer than expected
**Solution:**
- Reduce Lookback Days if data allows (e.g., from 7 to 1)
- Verify warehouse size is appropriate for data volume
- Consider adding clustering keys to target table on CUSTOMER_ID
- Switch from UNION to UNION ALL if exact duplicate detection not needed