# MultiSource Incremental Load

> Efficiently combine and incrementally load data from multiple sources with automatic deduplication in Coalesce

## Overview

Building fact tables from multiple source systems shouldn't require building complex DAGs with separate incremental nodes for each source, manual UNION logic, and custom deduplication. The MultiSource Incremental Load node template provides native multi-source support with intelligent incremental processing - all in a single, maintainable node.

## The Problem

When loading data from multiple sources into a fact table in Coalesce, teams typically face these challenges:

**Approach 1: Separate Nodes + Manual UNION**
- Create N incremental load nodes (one per source)
- Add downstream node to UNION results
- Write custom deduplication logic
- Maintain N+1 nodes with duplicate configuration

**Approach 2: Run View with Parameters**
- Complex parameter-based source filtering
- Manual join clause modifications
- Difficult to understand and maintain
- Limited incremental capabilities

**Result:** Bloated DAGs, increased maintenance overhead, higher compute costs, and error-prone deployments.

## The Solution

MultiSource Incremental Load consolidates everything into a single node with:

- **Native Multi-Source Support**: Combine 2-N sources using UNION or UNION ALL strategies
- **Intelligent Deduplication**: Automatic ROW_NUMBER() logic at source and combined levels
- **Efficient Incremental Processing**: Configurable lookback windows (1-90 days) prevent full table scans
- **Flexible Load Strategies**: Automatic MERGE for upsert or INSERT for append-only based on business keys
- **Initial Load Support**: Full historical load mode for first run, then switch to incremental
- **Built-in Audit**: Automatic LOAD_TIMESTAMP system column

## Use Cases

### Multi-Region Data Consolidation
Combine customer, order, or product data from regional databases (US, EU, APAC) into a single fact table with automatic deduplication by customer ID or order ID.

### Cross-System Integration
Merge product catalogs from ERP, PIM, and ecommerce platforms where the same products exist across systems with different last-modified timestamps.

### Business Unit Aggregation
Consolidate sales transactions from multiple business units or subsidiaries where each maintains their own operational database.

### CDC from Multiple Sources
Load change data capture feeds from multiple upstream systems with automatic handling of overlapping time windows.

## How It Works

### Multi-Level Deduplication

When business keys are defined, the node applies deduplication at two critical levels:

**Level 1: Source-Level Deduplication**
Within each source, keeps only the latest record per business key using ROW_NUMBER() ordered by load date column.

**Level 2: Combined-Level Deduplication**  
After unioning all sources, applies final deduplication across all sources to ensure only the most recent record per business key remains.

This ensures that if the same customer exists in both US and EU databases with different update timestamps, only the most recent version is loaded.

### Load Strategies

**With Business Keys (MERGE):**
```
1. Union all sources
2. Deduplicate within each source (keep latest per key)
3. Apply lookback window filter (optional)
4. Deduplicate across all sources (keep latest per key)
5. MERGE into target (UPDATE existing, INSERT new)
```

**Without Business Keys (INSERT):**
```
1. Union all sources
2. Apply lookback window filter
3. INSERT new records (append-only)
```

**Full Load (TRUNCATE/INSERT):**
```
1. Truncate target table
2. Union all sources  
3. Deduplicate (if business keys defined)
4. INSERT all records
```

## Configuration

### Node Properties

| Property | Description |
|----------|-------------|
| Storage Location | Target database/schema location |
| Node Type | MultiSource Incremental Fact |
| Description | Document the purpose of this fact table |
| Deploy Enabled | Enable/disable deployment |

### Core Options

| Option | Values | Description |
|--------|--------|-------------|
| **Multi Source** | True/False | Enable combining multiple sources via UNION |
| **Multi Source Strategy** | UNION / UNION ALL | UNION removes duplicates, UNION ALL keeps all rows |
| **Enable Incremental Loading** | True/False | Enable incremental processing vs full load |
| **Initial Full Load Mode** | True/False | Load all historical data (first run only) |
| **Load Date Column** | Column selector | Date/timestamp for incremental filtering and deduplication ordering |
| **Lookback Days** | 1,2,3,7,14,30,90 | How many days to look back for incremental loads |
| **Business Key Columns** | Column selector | Columns that uniquely identify business entities (enables MERGE) |

### Additional Options

| Option | Description |
|--------|-------------|
| **Enable Tests** | Run data quality tests before/after load |
| **Pre-SQL** | SQL to execute before data load |
| **Post-SQL** | SQL to execute after data load |

### System Columns

| Column | Type | Description |
|--------|------|-------------|
| LOAD_TIMESTAMP | TIMESTAMP | Automatically populated with current timestamp when record is loaded |

## Quick Start

### 1. Installation

Install the MultiSource Incremental Load node template from the Coalesce marketplace or import directly into your workspace.

### 2. Basic Setup (Multiple Sources)

1. Add 2+ source nodes to your DAG (e.g., CUSTOMER_US, CUSTOMER_EU, CUSTOMER_APAC)
2. Add a MultiSource Incremental Load node
3. Link all source nodes to the MultiSource node
4. Configure:
   - Enable **Multi Source** toggle
   - Select **UNION ALL** strategy
   - Map columns from all sources (ensure matching names/types)
5. Create and run the node

### 3. Add Incremental Processing

1. Enable **Enable Incremental Loading** toggle
2. Set **Initial Full Load Mode** to `True` for first run
3. Select **Load Date Column** (e.g., LAST_UPDATED_DATE)
4. Set **Lookback Days** (e.g., 7 for weekly refresh)
5. Run the node (loads all historical data)
6. Set **Initial Full Load Mode** to `False`
7. Run again (now processes only last 7 days)

### 4. Add Deduplication (MERGE)

1. Define **Business Key Columns** (e.g., CUSTOMER_ID)
2. Run the node
3. The node will now:
   - Deduplicate within each source
   - Deduplicate across all sources
   - MERGE into target (UPDATE existing, INSERT new)

## Examples

### Example 1: Multi-Region Customer Table

**Sources:**
- CUSTOMER_US (10M rows, updated daily)
- CUSTOMER_EU (5M rows, updated daily)
- CUSTOMER_APAC (3M rows, updated daily)

**Configuration:**
- Multi Source: True
- Strategy: UNION ALL
- Enable Incremental: True
- Load Date Column: LAST_MODIFIED_DATE
- Lookback Days: 1
- Business Keys: CUSTOMER_ID

**Result:** Single CUSTOMER_FACT table with deduplicated customers, processing only yesterday's changes across all three regions (~50K rows/day instead of 18M).

### Example 2: Cross-System Product Catalog

**Sources:**
- ERP_PRODUCTS (SKU, descriptions, costs)
- PIM_PRODUCTS (SKU, marketing content, images)
- ECOMMERCE_PRODUCTS (SKU, pricing, inventory)

**Configuration:**
- Multi Source: True
- Strategy: UNION
- Enable Incremental: True
- Load Date Column: MODIFIED_TIMESTAMP
- Lookback Days: 7
- Business Keys: SKU

**Result:** Consolidated product catalog with most recent data from whichever system was updated last.

## Performance Considerations

### Choosing Business Keys

**Good business key choices:**
- Natural keys with low cardinality relative to total rows (CUSTOMER_ID, ORDER_ID, SKU)
- Keys that actually represent the grain of your fact table
- Keys present in all source systems

**Poor business key choices:**
- High cardinality columns (TRANSACTION_ID with millions of unique values)
- Composite keys with 5+ columns (impacts window function performance)
- Columns with many NULL values

### Lookback Window Sizing

| Data Velocity | Recommended Lookback | Reasoning |
|---------------|---------------------|-----------|
| Real-time CDC | 1-2 days | Minimal latency, frequent updates |
| Daily batch | 3-7 days | Covers late-arriving data |
| Weekly batch | 7-14 days | Handles delayed source systems |
| Monthly batch | 30-90 days | Accommodates slow-moving dimensions |

**Rule of thumb:** Lookback should be 2-3x your longest expected source delay.

### UNION vs UNION ALL

| Use UNION when... | Use UNION ALL when... |
|-------------------|----------------------|
| Sources might have exact duplicate rows | You're certain no duplicates exist |
| You want Snowflake to remove duplicates | Performance is critical |
| Row count is relatively small (<1M) | Row volumes are large (10M+) |

**Note:** When business keys are defined, deduplication happens via ROW_NUMBER() regardless of UNION strategy. UNION vs UNION ALL only affects exact row duplicates.

## Comparison to Other Approaches

| Approach | Nodes | Auto Dedupe | Incremental | Maintenance | Performance |
|----------|-------|-------------|-------------|-------------|-------------|
| N separate incremental nodes + manual UNION | N+1 | No | Yes | High | Medium |
| Run View with parameters | N+1 | No | Limited | Medium | Medium |
| Looped Load | 2 | Manual | Yes | High | Low |
| **MultiSource Incremental Load** | **1** | **Yes** | **Yes** | **Low** | **High** |

## Deployment

### Initial Deployment

First deployment creates the target fact table with:
- All defined columns
- System columns (LOAD_TIMESTAMP)
- Table in specified storage location

### Redeployment

Subsequent deployments handle schema changes automatically:
- Adding columns
- Dropping columns  
- Changing data types
- Renaming table

Changes use clone/swap pattern to prevent data loss.

### Undeployment

Deleting the node from workspace and deploying drops:
- Internal Coalesce table
- Target Snowflake table

## Best Practices

### 1. Initial Load Strategy

Always run with **Initial Full Load Mode = True** on first execution to populate historical data, then switch to **False** for incremental processing.

### 2. Source Consistency

Ensure all sources have:
- Matching column names and data types
- The same Load Date Column present
- Business Key Columns present (if using MERGE)

### 3. Monitoring

Add **Enable Tests** with checks for:
- Row count thresholds
- NULL business keys
- Future-dated load timestamps
- Source distribution balance

### 4. Development Workflow

1. Build with **Multi Source = False** on single source first
2. Test incremental logic with one source
3. Enable **Multi Source = True** and add remaining sources
4. Validate deduplication logic with overlapping data
5. Tune **Lookback Days** based on actual source latency

## Troubleshooting

### Issue: No Data Loading

**Check:**
- Is **Initial Full Load Mode** still True after first run?
- Does **Lookback Days** cover the data's load date range?
- Are all sources returning data?

### Issue: Duplicates in Target

**Check:**
- Are **Business Key Columns** defined?
- Do business keys exist in all sources?
- Is **Multi Source Strategy** set correctly?

### Issue: Performance Degradation

**Check:**
- Are business keys appropriate (not too high cardinality)?
- Is **Lookback Days** too large?
- Should you use UNION ALL instead of UNION?
- Do you have appropriate clustering keys on target table?

## Technical Details

### Template Files

- **definition.yml**: Node configuration and UI controls
- **create.sql.j2**: CREATE TABLE/VIEW DDL generation
- **run.sql.j2**: MERGE/INSERT/TRUNCATE execution logic

### Dependencies

- Coalesce version 4.0+
- Snowflake (uses ROW_NUMBER, MERGE, QUALIFY)

### SQL Pattern

The run template uses Snowflake's QUALIFY clause for efficient deduplication:

```sql
SELECT *
FROM (
    SELECT 
        columns...,
        ROW_NUMBER() OVER (
            PARTITION BY business_key 
            ORDER BY load_date DESC
        ) AS rn
    FROM sources...
)
WHERE rn = 1
```

This approach pushes deduplication to Snowflake's query optimizer rather than using subqueries or CTEs.


**Feature Requests:**  
Submit via GitHub issues with [FEATURE REQUEST] tag

**Contributing:**  
Pull requests welcome for bug fixes, documentation improvements, and new features

## License

MIT License - See LICENSE file for details

## Acknowledgments

Built for the Coalesce community by data engineers who were tired of maintaining complex multi-source DAGs.

---

**Questions?** Open an issue or discussion on GitHub.