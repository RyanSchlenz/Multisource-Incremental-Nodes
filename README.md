# MultiSource Incremental Fact

Efficiently combine and incrementally load data from multiple sources into a single physical fact table in Coalesce, with optional soft delete handling.

## Overview

Building fact tables from multiple source systems in Coalesce typically means building complex DAGs with separate incremental nodes per source, manual UNION logic, and downstream deduplication. The MultiSource Incremental Fact node template provides native multi-source support with intelligent incremental processing in a single, maintainable node.

## The Problem

When loading data from multiple sources into a fact table in Coalesce, teams typically face these challenges:

**Approach 1: Separate Nodes + Manual UNION**
Create N incremental load nodes (one per source), add a downstream node to UNION results, write custom deduplication logic, and maintain N+1 nodes with duplicate configuration.

**Approach 2: Run View with Parameters**
Complex parameter-based source filtering, manual join clause modifications, difficult to understand and maintain, and limited incremental capabilities. Materializes as a view rather than a physical table.

**Result:** Bloated DAGs, increased maintenance overhead, higher compute costs, and error-prone deployments.

## The Solution

MultiSource Incremental Fact consolidates everything into a single node:

- **Native Multi-Source Support**: Combine 2-N sources using UNION or UNION ALL strategies
- **Efficient Incremental Processing**: Auto-detecting lookback window eliminates manual initial load toggling
- **Flexible Load Strategies**: Automatic MERGE (upsert) when business keys are defined, INSERT for append-only
- **Soft Delete Handling**: Optional DELETE pathway in MERGE for Fivetran-style soft deletes
- **Fail-Loud Design**: No template-level deduplication -- data quality issues surface immediately rather than being silently masked
- **Built-in Audit**: Automatic LOAD_TIMESTAMP system column

## Design Philosophy

This template intentionally does not perform deduplication at the template level. If duplicate business keys exist in the source data, the MERGE operation will fail rather than silently picking a winner. This is by design.

**Why:** Deduplication belongs upstream in your data pipeline (satellites, staging views, source transformations). When the template masks duplicates, you never discover the upstream data quality issue that caused them. When it fails loudly, you investigate, fix the root cause, and your pipeline is healthier for it.

If your sources produce duplicates, resolve them at the source branch level (in joins or source transforms) before they reach this node.

## How It Works

### Incremental Processing

The template uses auto-detecting lookback to handle both initial and incremental loads without manual toggling:


WHERE load_date >= COALESCE(
    DATEADD(day, -3, (SELECT MAX(load_date) FROM target_table)),
    '1900-01-01'::TIMESTAMP
)


On first run (empty table), `MAX(load_date)` returns NULL. The `COALESCE` falls back to `1900-01-01`, loading all historical data automatically. On subsequent runs, it looks back 3 days from the most recent record in the target, catching any late-arriving data.

No toggle to flip. No forgetting to switch from initial to incremental mode.

### Load Strategies

**With Business Keys (MERGE):**


1. Union all sources
2. Apply lookback window filter (auto-detected)
3. MERGE into target:
   - UPDATE existing records
   - INSERT new records
   - DELETE soft-deleted records (if enabled)


**Without Business Keys (INSERT):**


1. Union all sources
2. Apply lookback window filter (auto-detected)
3. Filter out soft-deleted records (if enabled)
4. INSERT new records (append-only)


**Full Load (TRUNCATE/INSERT):**


1. Truncate target table
2. Union all sources
3. Filter out soft-deleted records (if enabled)
4. INSERT all records


### Soft Delete Handling

When enabled, the template handles source systems that use soft delete flags (such as Fivetran's `_FIVETRAN_DELETED`).

**In the MERGE path**, the soft delete column flows through the source query into the MERGE. Three pathways handle it:

- Records where the flag is TRUE get deleted from the fact table
- Records where the flag is FALSE get updated or inserted normally
- The soft delete column itself is excluded from the physical fact table

**In INSERT and full load paths**, soft-deleted records are filtered out via WHERE clause before insertion.

The soft delete column must be mapped as a column on the node in Coalesce so the template can reference it, but it never appears in the target table DDL. The fact table stays clean.

**Important:** The soft delete column typically comes from a current satellite in Data Vault architectures. Hubs do not carry delete flags. When using this feature, ensure your source join includes the satellite containing the soft delete column, and remove any existing WHERE clause that filters on that column (the template handles it).

## Configuration

### Node Properties

| Property | Description |
| --- | --- |
| Storage Location | Target database/schema location |
| Node Type | MultiSource Incremental Fact |
| Description | Document the purpose of this fact table |
| Deploy Enabled | Enable/disable deployment |

### Core Options

| Option | Values | Description |
| --- | --- | --- |
| **Multi Source** | True/False | Enable combining multiple sources via UNION |
| **Multi Source Strategy** | UNION / UNION ALL | UNION removes exact duplicate rows, UNION ALL keeps all rows |
| **Enable Incremental Loading** | True/False | Enable incremental processing vs full truncate/reload |
| **Load Date Column** | Column selector | Date/timestamp column for incremental filtering and lookback calculation |
| **Business Key Columns** | Column selector | Columns that uniquely identify business entities (enables MERGE) |
| **Enable Soft Delete Handling** | True/False | Enable DELETE pathway for soft-deleted source records |
| **Soft Delete Column** | Column selector | Boolean column indicating soft deletes (e.g., _FIVETRAN_DELETED) |

### Additional Options

| Option | Description |
| --- | --- |
| **Enable Tests** | Run data quality tests before and/or after load |
| **Pre-SQL** | SQL to execute before data load |
| **Post-SQL** | SQL to execute after data load |

### System Columns

| Column | Type | Description |
| --- | --- | --- |
| LOAD_TIMESTAMP | TIMESTAMP | Automatically populated with current timestamp when record is loaded |

## Quick Start

### 1. Basic Setup (Multiple Sources)

1. Add 2+ source nodes to your DAG (e.g., SOURCE_A, SOURCE_B, SOURCE_C)
2. Add a MultiSource Incremental Fact node
3. Link all source nodes to the node
4. Configure:
   - Enable **Multi Source** toggle
   - Select **UNION ALL** strategy
   - Map columns from all sources (ensure matching names/types)
5. Create and run the node

### 2. Add Incremental Processing

1. Enable **Enable Incremental Loading** toggle
2. Select **Load Date Column** (e.g., LOAD_DATE, LAST_UPDATED_DATE)
3. Run the node -- it automatically detects the empty table and loads all historical data
4. Run again -- it now processes only the last 3 days from the most recent record

No mode switching required. The auto-detect handles initial vs incremental automatically.

### 3. Add MERGE (Upsert)

1. Define **Business Key Columns** (e.g., REQUEST_ID, CUSTOMER_ID)
2. Run the node
3. The node will now MERGE into the target: UPDATE existing records, INSERT new ones

### 4. Add Soft Delete Handling (Optional)

1. Map the soft delete column (e.g., _FIVETRAN_DELETED) as a column on the node
2. Enable **Enable Soft Delete Handling** toggle
3. Select the soft delete column in the **Soft Delete Column** dropdown
4. Remove any existing WHERE clause filtering on the soft delete column from your source joins -- the template handles this now
5. Run the node

## Use Cases

### Multi-Region Data Consolidation

Combine customer, order, or product data from regional databases (US, EU, APAC) into a single fact table. Business keys prevent duplicate entries when the same entity exists across regions.

### Cross-System Integration

Merge data from ERP, CRM, and operational platforms where the same business entities appear across systems with different update timestamps.

### Data Vault Fact Loading

Load fact tables from multiple current satellite views, with each source branch contributing columns from different aspects of the business entity. Soft delete handling ensures records deleted in source systems are properly removed from the fact table.

### CDC from Multiple Sources

Load change data capture feeds from multiple upstream systems. The auto-detecting lookback window handles late-arriving data without manual intervention.

## Performance Considerations

### Lookback Window

The hardcoded 3-day lookback from MAX(load_date) balances two concerns: catching late-arriving data and minimizing the volume of records processed per run. For most daily-batch pipelines, 3 days provides sufficient coverage. If your sources have longer delays, adjust the lookback value in the template.

### UNION vs UNION ALL

| Use UNION when... | Use UNION ALL when... |
| --- | --- |
| Sources might produce exact duplicate rows | You are certain no duplicates exist across sources |
| Row count is relatively small | Performance is critical and volumes are large |

### Business Key Selection

Choose business keys that represent the actual grain of your fact table. These should be present in all source systems and uniquely identify each business entity. Composite business keys (e.g., REQUEST_ID + DSS_RECORD_SOURCE) are supported for preventing cross-source collisions.

## Comparison to Other Approaches

| Approach | Nodes | Incremental | Soft Delete | Physical Table | Maintenance |
| --- | --- | --- | --- | --- | --- |
| N separate incremental nodes + manual UNION | N+1 | Yes | No | Yes | High |
| Run View with parameters | N+1 | Limited | No | No (view) | Medium |
| Looped Load | 2 | Yes | No | Yes | High |
| **MultiSource Incremental Fact** | **1** | **Yes** | **Yes** | **Yes** | **Low** |

## Deployment

### Initial Deployment

First deployment creates the target fact table with all defined columns (excluding the soft delete column if enabled), LOAD_TIMESTAMP system column, and table in the specified storage location.

### Redeployment

Subsequent deployments handle schema changes automatically using Coalesce's clone/swap pattern: adding columns, dropping columns, changing data types, and renaming tables. No data is lost during schema changes.

### Undeployment

Deleting the node from the workspace and deploying drops the internal Coalesce table and the target Snowflake table.

## Troubleshooting

### No Data Loading on Incremental Runs

Check whether the Load Date Column values in source data fall within the 3-day lookback window from the MAX value in the target table. If source data is older than 3 days past the most recent target record, it will be excluded.

### MERGE Fails with Duplicate Key Error

This is expected behavior. The template does not deduplicate -- duplicates in source data cause the MERGE to fail. Investigate and fix the upstream source that is producing duplicate business keys. Common causes: missing QUALIFY clause in source joins, fan-out from many-to-many relationships in link tables, or incorrect grain in source branch queries.

### Deleted Records Persisting in Fact Table

Verify that **Enable Soft Delete Handling** is turned on and the correct column is selected. Confirm that you removed any WHERE clause filtering on the soft delete column from your source joins. The template needs to see the deleted records in order to act on them.

### Records Missing After Enabling Soft Delete

The soft delete column must be mapped on the node. If it is not present in the column mapping, the template cannot reference it. Add the column to the node mapping, select it in the Soft Delete Column dropdown, and rerun.

## Technical Details

### Template Files

| File | Purpose |
| --- | --- |
| definition.yml | Node configuration, UI controls, and system columns |
| create.sql.j2 | CREATE TABLE DDL generation (excludes soft delete column) |
| run.sql.j2 | MERGE/INSERT/TRUNCATE execution logic |

### Dependencies

- Coalesce version 4.0+
- Snowflake (uses MERGE, COALESCE, DATEADD)

### Key SQL Patterns

**Auto-detecting lookback (replaces manual initial load toggle):**

WHERE load_date >= COALESCE(
    DATEADD(day, -3, (SELECT MAX(load_date) FROM target)),
    '1900-01-01'::TIMESTAMP
)


**Three-pathway MERGE with soft delete:**


MERGE INTO target USING source
ON business_key_match
WHEN MATCHED AND _FIVETRAN_DELETED = TRUE THEN DELETE
WHEN MATCHED AND _FIVETRAN_DELETED = FALSE THEN UPDATE SET ...
WHEN NOT MATCHED AND _FIVETRAN_DELETED = FALSE THEN INSERT ...


## Best Practices

### 1. Source Data Quality

Ensure upstream sources (satellites, staging views, source transforms) produce deduplicated data at the correct grain before it reaches this node. The template enforces this by failing on duplicates rather than masking them.

### 2. Source Consistency

All sources must have matching column names and data types, the same Load Date Column present, and Business Key Columns present if using MERGE.

### 3. Soft Delete Column Handling

Map the soft delete column on the node but do not add manual WHERE clauses filtering on it in the source joins. Let the template handle the filtering. This ensures the MERGE DELETE pathway can see and act on deleted records.

### 4. Monitoring

Use the **Enable Tests** toggle with Pre-SQL and Post-SQL for row count thresholds, NULL business key checks, and source distribution balance. The template runs before-tests prior to data loading and after-tests upon completion.

### 5. Development Workflow

1. Build with **Multi Source = False** on a single source first
2. Test incremental logic with one source
3. Enable **Multi Source = True** and add remaining sources
4. Validate output against existing fact tables
5. Enable soft delete handling if applicable
6. Run in parallel with existing pipeline before cutover

## License

MIT License - See LICENSE file for details.
