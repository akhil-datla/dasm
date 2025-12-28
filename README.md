# DASM - Data Assembly Language

A minimalistic, embeddable assembly-like bytecode language for dataframe operations. Think "Starlark for DataFrames" — a sandboxed, deterministic, Go-embeddable language that provides Pandas-like capabilities through a custom instruction set and virtual machine.

## Features

- **Register-based bytecode VM** - Fast execution with 16 scalar registers, 8 vector registers, and 16 float registers
- **Assembly language** - Low-level control with readable `.dasm` syntax
- **High-level DSL** - Optional Pandas-like syntax that compiles to assembly
- **Go embedding API** - Simple API: `Execute()`, `ExecuteFile()`, `ExecuteWithFrames()`
- **Multi-format import** - Load data from CSV, JSON, and Parquet files
- **Sandboxing** - Resource limits, timeouts, and file access controls
- **Optimizer** - Constant folding, projection pruning, predicate pushdown, dead code elimination
- **Interactive REPL** - Explore data interactively with assembly or DSL modes
- **DataFrame operations** - Filter, aggregate, group by, join, string operations

## Installation

### Pre-built Binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/akhildatla/dasm/releases):

| Platform | Architecture | Download |
|----------|--------------|----------|
| Linux | amd64 | `dasm_VERSION_linux_amd64.tar.gz` |
| Linux | arm64 | `dasm_VERSION_linux_arm64.tar.gz` |
| Linux | armv6 | `dasm_VERSION_linux_armv6.tar.gz` |
| Linux | armv7 | `dasm_VERSION_linux_armv7.tar.gz` |
| Linux | 386 | `dasm_VERSION_linux_386.tar.gz` |
| macOS | amd64 (Intel) | `dasm_VERSION_darwin_amd64.tar.gz` |
| macOS | arm64 (Apple Silicon) | `dasm_VERSION_darwin_arm64.tar.gz` |
| Windows | amd64 | `dasm_VERSION_windows_amd64.zip` |
| Windows | arm64 | `dasm_VERSION_windows_arm64.zip` |
| Windows | 386 | `dasm_VERSION_windows_386.zip` |

Extract and add to your PATH:

```bash
# Linux/macOS
tar -xzf dasm_VERSION_OS_ARCH.tar.gz
sudo mv dasm /usr/local/bin/

# Windows (PowerShell)
Expand-Archive dasm_VERSION_windows_amd64.zip
Move-Item dasm.exe C:\Windows\System32\
```

### Go Install

```bash
go install github.com/akhildatla/dasm/cmd/dasm@latest
```

### Build from Source

```bash
git clone https://github.com/akhildatla/dasm.git
cd dasm
go build ./cmd/dasm
```

## Quick Start

### CLI Usage

```bash
# Run an assembly program
dasm run program.dasm

# Run with built-in example data
dasm run -example-frames program.dasm

# Compile to bytecode
dasm compile program.dasm -o program.dfbc

# Compile with optimizations
dasm compile -O -v program.dasm

# Execute bytecode
dasm exec program.dfbc

# Disassemble bytecode
dasm disasm program.dfbc

# Start interactive REPL
dasm repl

# REPL with example data in assembly mode
dasm repl -example-frames -asm
```

### Example Program

Create `sum_prices.dasm`:

```asm
; Sum all prices from a CSV file
LOAD_CSV      R0, "sales.csv"
SELECT_COL    V0, R0, "price"
REDUCE_SUM_F  F0, V0
HALT_F        F0
```

Run it:

```bash
dasm run sum_prices.dasm
# Output: 65.5
```

## Go Embedding API

### Basic Execution

```go
import "github.com/akhildatla/dasm/pkg/embed"

// Execute assembly code
result, err := embed.Execute(`
    LOAD_CONST R0, 42
    HALT R0
`)
fmt.Println(result) // 42
```

### With DataFrames

```go
import (
    "github.com/akhildatla/dasm/pkg/embed"
    dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Create a DataFrame
frame := dataframe.NewDataFrame(
    dataframe.NewSeriesFloat64("price", nil, 10.0, 20.0, 30.0),
    dataframe.NewSeriesInt64("quantity", nil, 2, 3, 4),
)

// Execute with predeclared frames
result, err := embed.ExecuteWithFrames(`
    LOAD_FRAME    R0, "sales"
    SELECT_COL    V0, R0, "price"
    SELECT_COL    V1, R0, "quantity"
    VEC_MUL_F     V2, V0, V1
    REDUCE_SUM_F  F0, V2
    HALT_F        F0
`, map[string]*dataframe.DataFrame{"sales": frame})

fmt.Printf("Total: %.2f\n", result) // Total: 200.00
```

### With Options (Sandboxing)

```go
result, err := embed.ExecuteWithOptions(`
    LOAD_CSV R0, "allowed/data.csv"
    SELECT_COL V0, R0, "value"
    REDUCE_SUM_F F0, V0
    HALT_F F0
`,
    embed.WithMaxInstructions(10000),
    embed.WithTimeout(5*time.Second),
    embed.WithSandbox(true),
    embed.WithAllowedPaths("allowed/"),
)
```

### Execute DSL

```go
result, err := embed.ExecuteDSL(`
    data = frame("sales")
    filtered = data.price > 100
    count = count(filtered)
    return count
`)
```

## Assembly Language Reference

### Registers

| Type | Registers | Purpose |
|------|-----------|---------|
| R | R0-R15 | 64-bit integer scalars, frame references |
| F | F0-F15 | 64-bit float scalars |
| V | V0-V7 | Vector registers (column data) |

### Opcodes

#### Data Loading
```asm
LOAD_CSV      R0, "file.csv"      ; Load CSV into frame
LOAD_JSON     R0, "file.json"     ; Load JSON into frame
LOAD_PARQUET  R0, "file.parquet"  ; Load Parquet into frame
LOAD_FRAME    R0, "name"          ; Load predeclared frame
LOAD_CONST    R0, 42              ; Load integer constant
LOAD_CONST_F  F0, 3.14            ; Load float constant
SELECT_COL    V0, R0, "column"    ; Select column from frame
BROADCAST     V0, R1, V1          ; Broadcast scalar to vector length
BROADCAST_F   V0, F1, V1          ; Broadcast float to vector length
```

#### Vector Arithmetic
```asm
VEC_ADD_I     V0, V1, V2          ; Integer addition
VEC_SUB_I     V0, V1, V2          ; Integer subtraction
VEC_MUL_I     V0, V1, V2          ; Integer multiplication
VEC_DIV_I     V0, V1, V2          ; Integer division
VEC_MOD_I     V0, V1, V2          ; Integer modulo
VEC_ADD_F     V0, V1, V2          ; Float addition
VEC_SUB_F     V0, V1, V2          ; Float subtraction
VEC_MUL_F     V0, V1, V2          ; Float multiplication
VEC_DIV_F     V0, V1, V2          ; Float division
```

#### Comparison (produces bool vector)
```asm
CMP_EQ        V0, V1, V2          ; Equal
CMP_NE        V0, V1, V2          ; Not equal
CMP_LT        V0, V1, V2          ; Less than
CMP_LE        V0, V1, V2          ; Less than or equal
CMP_GT        V0, V1, V2          ; Greater than
CMP_GE        V0, V1, V2          ; Greater than or equal
```

#### Logical
```asm
AND           V0, V1, V2          ; Logical AND
OR            V0, V1, V2          ; Logical OR
NOT           V0, V1              ; Logical NOT
```

#### Filtering
```asm
FILTER        V0, V1, V2          ; Filter V1 by bool mask V2
TAKE          V0, V1, V2          ; Take elements at indices
```

#### Aggregations
```asm
REDUCE_SUM    R0, V1              ; Sum (integer result)
REDUCE_SUM_F  F0, V1              ; Sum (float result)
REDUCE_COUNT  R0, V1              ; Count elements
REDUCE_MIN    R0, V1              ; Minimum (integer)
REDUCE_MAX    R0, V1              ; Maximum (integer)
REDUCE_MIN_F  F0, V1              ; Minimum (float)
REDUCE_MAX_F  F0, V1              ; Maximum (float)
REDUCE_MEAN   F0, V1              ; Mean (float)
```

#### GroupBy
```asm
GROUP_BY      R1, V0              ; Group by key column
GROUP_SUM     V2, R1, V1          ; Sum per group
GROUP_SUM_F   V2, R1, V1          ; Sum per group (float)
GROUP_COUNT   V2, R1              ; Count per group
GROUP_MIN     V2, R1, V1          ; Min per group
GROUP_MAX     V2, R1, V1          ; Max per group
GROUP_MIN_F   V2, R1, V1          ; Min per group (float)
GROUP_MAX_F   V2, R1, V1          ; Max per group (float)
GROUP_MEAN    V2, R1, V1          ; Mean per group
GROUP_KEYS    V2, R1              ; Get unique keys
```

#### Join
```asm
JOIN_INNER    R2, R0, R1, "key"   ; Inner join on key column
JOIN_LEFT     R2, R0, R1, "key"   ; Left join
JOIN_RIGHT    R2, R0, R1, "key"   ; Right join
JOIN_OUTER    R2, R0, R1, "key"   ; Outer join
```

#### String Operations
```asm
STR_LEN       V1, V0              ; String length
STR_UPPER     V1, V0              ; Uppercase
STR_LOWER     V1, V0              ; Lowercase
STR_TRIM      V1, V0              ; Trim whitespace
STR_CONCAT    V2, V0, V1          ; Concatenate strings
STR_CONTAINS  V1, V0, "pattern"   ; Contains substring
STR_STARTS_WITH V1, V0, "prefix"  ; Starts with
STR_ENDS_WITH V1, V0, "suffix"    ; Ends with
STR_SPLIT     V1, V0, ","         ; Split by delimiter
STR_REPLACE   V1, V0, "old", "new"; Replace substring
```

#### Frame Operations
```asm
NEW_FRAME     R0                  ; Create empty frame
ADD_COL       R0, V1, "name"      ; Add column to frame
ROW_COUNT     R1, R0              ; Get row count
COL_COUNT     R1, R0              ; Get column count
```

#### Scalar Operations
```asm
MOVE_R        R0, R1              ; Copy register
MOVE_F        F0, F1              ; Copy float register
ADD_R         R0, R1, R2          ; Add integers
SUB_R         R0, R1, R2          ; Subtract integers
MUL_R         R0, R1, R2          ; Multiply integers
DIV_R         R0, R1, R2          ; Divide integers
```

#### Control Flow
```asm
NOP                               ; No operation
HALT          R0                  ; Stop, return R0
HALT_F        F0                  ; Stop, return F0
```

## High-Level DSL

DASM includes an optional high-level DSL that compiles to assembly:

```python
# Load and process data
data = frame("sales")
prices = data.price
quantities = data.quantity
total = prices * quantities
result = sum(total)
return result
```

### DSL Syntax

#### Data Loading
```python
# Load from predeclared frames
data = frame("sales")

# Load from CSV file
data = load("sales.csv")

# Load from JSON file
data = load_json("data.json")

# Load from Parquet file
data = load_parquet("data.parquet")
```

#### Column Access
```python
data = frame("sales")
prices = data.price           # dot notation
quantities = data.quantity
```

#### Arithmetic Operations
```python
total = prices * quantities   # multiplication
profit = revenue - cost       # subtraction
ratio = a / b                 # division
sum_val = x + y               # addition
remainder = x % 5             # modulo
```

#### Comparison Operators
```python
expensive = prices > 100      # greater than
cheap = prices <= 10          # less than or equal
same = a == b                 # equal
different = a != b            # not equal
```

#### Logical Operators
```python
combined = cond1 and cond2    # logical AND (also &&)
either = cond1 or cond2       # logical OR (also ||)
inverted = not cond           # logical NOT (also !)
```

#### Aggregation Functions
```python
total = sum(prices)           # sum of values
n = count(data)               # count of elements
avg = mean(prices)            # average (also avg)
smallest = min(prices)        # minimum value
largest = max(prices)         # maximum value
```

#### String Functions
```python
upper_names = upper(names)         # uppercase
lower_names = lower(names)         # lowercase
trimmed = trim(text)               # trim whitespace
length = len(names)                # string length (also length)
has_son = contains(names, "son")   # contains substring
starts = starts_with(names, "A")   # starts with prefix
ends = ends_with(names, "son")     # ends with suffix
full = concat(first, last)         # concatenate strings
parts = split(text, ",")           # split by delimiter
fixed = replace(text, "old", "new") # replace substring
```

#### Filtering
```python
# Filter with boolean expression
filtered = filter(data, data.price > 100)

# Using 'where' alias
filtered = where(data, data.quantity >= 10)
```

#### Mutate (Add Computed Columns)
```python
# Add computed column
data = mutate(data, total = data.price * data.quantity)
```

#### GroupBy and Summarize
```python
# Group by category and summarize
grouped = group_by(data, data.category)
result = summarize(grouped, total = sum(data.amount), n = count(data))
```

#### Joins
```python
# Inner join using pipe syntax
combined = frame("orders") |> join(frame("customers"), on: customer_id)

# Left join
combined = frame("orders") |> left_join(frame("customers"), on: customer_id)

# Right join
combined = frame("orders") |> right_join(frame("customers"), on: customer_id)

# Outer join (full outer)
combined = frame("orders") |> outer_join(frame("customers"), on: customer_id)
```

#### Frame Operations
```python
# Create an empty frame
result = new_frame()

# Add column to frame
result = add_col(result, "name", column_data)

# Get row count
n_rows = row_count(data)

# Get column count
n_cols = col_count(data)
```

#### Index Operations
```python
# Take first N elements from a vector
first_10 = take(data.price, 10)
```

#### Return Statement
```python
# Return the final result
return result
```

### DSL Examples

#### Sum Prices Above Threshold
```python
data = frame("sales")
high_value = filter(data, data.price > 50)
total = sum(high_value.price)
return total
```

#### Calculate Revenue by Category
```python
data = frame("sales")
data = mutate(data, revenue = data.price * data.quantity)
grouped = group_by(data, data.category)
result = summarize(grouped, total_revenue = sum(data.revenue))
return result
```

#### Join and Aggregate
```python
combined = frame("orders") |> join(frame("customers"), on: customer_id)
total = sum(combined.amount)
return total
```

#### String Filtering
```python
data = frame("people")
names_with_son = filter(data, ends_with(data.name, "son"))
count_result = count(names_with_son)
return count_result
```

#### Complex Pipeline
```python
data = frame("products")

# Filter by price and category
expensive = filter(data, data.price > 50)
category_b = filter(expensive, contains(data.category, "B"))

# Check if in stock
available = filter(category_b, data.in_stock == true)

# Count results
result = count(available)
return result
```

#### Load and Process JSON
```python
data = load_json("sales.json")
filtered = filter(data, data.amount > 100)
total = sum(filtered.amount)
return total
```

#### Build a Result Frame
```python
data = frame("sales")
grouped = group_by(data, data.category)
totals = summarize(grouped, total = sum(data.amount), n = count(data))

# Get row count of result
n_rows = row_count(totals)
return n_rows
```

## Examples

### Filter and Aggregate

```asm
; Average price where quantity > 10
LOAD_CSV      R0, "sales.csv"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
LOAD_CONST    R1, 10
BROADCAST     V2, R1, V1
CMP_GT        V3, V1, V2          ; quantity > 10
FILTER        V4, V0, V3          ; filtered prices
REDUCE_MEAN   F0, V4              ; average
HALT_F        F0
```

### Computed Column

```asm
; Calculate total = price * quantity and sum
LOAD_CSV      R0, "sales.csv"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "quantity"
VEC_MUL_F     V2, V0, V1          ; total = price * quantity
REDUCE_SUM_F  F0, V2
HALT_F        F0
```

### GroupBy Aggregation

```asm
; Sum sales by category
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "category"
SELECT_COL    V1, R0, "amount"
GROUP_BY      R1, V0              ; group by category
GROUP_SUM     V2, R1, V1          ; sum per group
REDUCE_SUM    R2, V2              ; total across all groups
HALT          R2
```

### Multiple Aggregations per Group

```asm
; Get count, sum, and mean per category
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "category"
SELECT_COL    V1, R0, "amount"
GROUP_BY      R1, V0
GROUP_COUNT   V2, R1              ; count per group
GROUP_SUM_F   V3, R1, V1          ; sum per group
GROUP_MEAN    V4, R1, V1          ; mean per group
; Results in V2, V3, V4 correspond to GROUP_KEYS V5, R1
REDUCE_SUM_F  F0, V3
HALT_F        F0
```

### Join Tables

```asm
; Join orders with customers and sum amounts
LOAD_FRAME    R0, "orders"
LOAD_FRAME    R1, "customers"
JOIN_INNER    R2, R0, R1, "customer_id"
SELECT_COL    V0, R2, "amount"
REDUCE_SUM_F  F0, V0
HALT_F        F0
```

### Left Join with Aggregation

```asm
; Left join to include all orders, even without customer match
LOAD_FRAME    R0, "orders"
LOAD_FRAME    R1, "customers"
JOIN_LEFT     R2, R0, R1, "customer_id"
SELECT_COL    V0, R2, "amount"
REDUCE_COUNT  R3, V0
HALT          R3
```

### String Filtering

```asm
; Count names ending with "son"
LOAD_FRAME    R0, "people"
SELECT_COL    V0, R0, "name"
STR_ENDS_WITH V1, V0, "son"
REDUCE_COUNT  R1, V1
HALT          R1
```

### Complex String Processing

```asm
; Convert names to uppercase and check if contains "SON"
LOAD_FRAME    R0, "people"
SELECT_COL    V0, R0, "name"
STR_UPPER     V1, V0              ; uppercase
STR_CONTAINS  V2, V1, "SON"       ; contains "SON"
FILTER        V3, V0, V2          ; filter original names
REDUCE_COUNT  R1, V3
HALT          R1
```

### Multi-Condition Filtering

```asm
; Products where price > 50 AND category = "B" AND in_stock = true
LOAD_FRAME    R0, "products"
SELECT_COL    V0, R0, "price"
SELECT_COL    V1, R0, "category"
SELECT_COL    V2, R0, "in_stock"

; price > 50
LOAD_CONST_F  F0, 50.0
BROADCAST_F   V3, F0, V0
CMP_GT        V4, V0, V3

; category == "B" (using string contains as equality proxy)
STR_CONTAINS  V5, V1, "B"

; Combine conditions
AND           V6, V4, V5          ; price > 50 AND category contains B
AND           V7, V6, V2          ; AND in_stock

; Filter and count
FILTER        V8, V0, V7
REDUCE_COUNT  R1, V8
HALT          R1
```

### Load from JSON

```asm
; Load data from JSON array file
LOAD_JSON     R0, "data.json"
SELECT_COL    V0, R0, "value"
REDUCE_SUM_F  F0, V0
HALT_F        F0
```

### Load from Parquet

```asm
; Load data from Parquet file
LOAD_PARQUET  R0, "data.parquet"
SELECT_COL    V0, R0, "amount"
REDUCE_MEAN   F0, V0
HALT_F        F0
```

### Building a Result Frame

```asm
; Create output frame with aggregated results
LOAD_FRAME    R0, "sales"
SELECT_COL    V0, R0, "category"
SELECT_COL    V1, R0, "amount"

GROUP_BY      R1, V0
GROUP_KEYS    V2, R1              ; unique categories
GROUP_SUM_F   V3, R1, V1          ; sum per category
GROUP_COUNT   V4, R1              ; count per category

NEW_FRAME     R2
ADD_COL       R2, V2, "category"
ADD_COL       R2, V3, "total"
ADD_COL       R2, V4, "count"

ROW_COUNT     R3, R2
HALT          R3
```

## Interactive REPL

Start the REPL with:

```bash
# DSL mode (default)
dasm repl

# Assembly mode
dasm repl -asm

# With example data
dasm repl -example-frames
```

### REPL Commands

- `:mode asm` - Switch to assembly mode
- `:mode dsl` - Switch to DSL mode
- `:frames` - List available frames
- `:clear` - Clear the screen
- `:help` - Show help
- `:quit` or `:exit` - Exit REPL

### Example REPL Session

```
dasm> :mode asm
Switched to ASM mode
dasm(asm)> LOAD_CONST R0, 42
R0 = 42
dasm(asm)> LOAD_CONST R1, 8
R1 = 8
dasm(asm)> ADD_R R2, R0, R1
R2 = 50
```

## Bytecode Optimization

Compile with optimizations enabled:

```bash
dasm compile -O -v program.dasm -o program.dfbc
```

### Optimization Passes

1. **Constant Folding** - Evaluates constant expressions at compile time
2. **Dead Code Elimination** - Removes instructions that don't affect the result
3. **Projection Pruning** - Removes unused column selections
4. **Predicate Pushdown** - Moves filters closer to data source

### Example

```asm
; Before optimization
LOAD_CONST R0, 10
LOAD_CONST R1, 20
LOAD_CONST R2, 5      ; dead - never used
LOAD_CONST R3, 100    ; dead - never used
ADD_R R4, R0, R1
HALT R4

; After optimization (-O flag)
LOAD_CONST R0, 10
LOAD_CONST R1, 20
ADD_R R4, R0, R1
HALT R4
```

## Architecture

```
+------------------------------------------------------------------+
|                        DASM Architecture                          |
+------------------------------------------------------------------+
|                                                                   |
|   User Code              Compiler Pipeline         Execution      |
|                                                                   |
|   +----------+     +-------+     +---------+     +----------+    |
|   | Assembly | --> | Lexer | --> | Parser/ | --> | Bytecode |    |
|   |  .dasm   |     |       |     | Compiler|     |    VM    |    |
|   +----------+     +-------+     +---------+     +----------+    |
|                                        |              ^           |
|   +----------+     +-------+     +-----v----+         |           |
|   | High-DSL | --> | Lexer | --> | Compiler | --------+           |
|   |          |     |       |     | to ASM   |                     |
|   +----------+     +-------+     +----------+                     |
|                                                                   |
|   +-----------------------------------------------------------+  |
|   |              DataFrame Storage (dataframe-go)              |  |
|   |        Supports: CSV, JSON, Parquet, SQL, Excel            |  |
|   +-----------------------------------------------------------+  |
|                                                                   |
+------------------------------------------------------------------+
```

## Project Structure

```
dasm/
├── cmd/dasm/           # CLI tool
├── pkg/
│   ├── compiler/       # Assembly lexer, parser, compiler
│   ├── dsl/            # High-level DSL
│   ├── embed/          # Go embedding API
│   ├── loader/         # CSV, JSON, Parquet loaders
│   ├── optimizer/      # Optimization passes
│   ├── repl/           # Interactive REPL
│   └── vm/             # Virtual machine
├── examples/           # Example programs
└── testdata/           # Test fixtures
```

## Testing

```bash
# Run all tests
go test ./...

# With coverage
go test ./... -cover

# Specific package
go test ./pkg/vm/... -v

# Run benchmarks
go test ./pkg/vm/... -bench=. -benchmem
```

## Built-in Example Frames

When using `-example-frames` flag, these frames are available:

| Frame | Columns | Description |
|-------|---------|-------------|
| `sales` | category, amount | Sales data with 4 rows |
| `people` | name, age | People data with 5 rows |
| `orders` | order_id, customer_id, amount | Order data with 5 rows |
| `customers` | customer_id, name | Customer data with 3 rows |
| `products` | name, price, category, in_stock | Product catalog with 5 rows |

## Supported File Formats

| Format | Load Instruction | Notes |
|--------|------------------|-------|
| CSV | `LOAD_CSV` | First row is header, auto-detects types |
| JSON | `LOAD_JSON` | Must be array of objects: `[{...}, {...}]` |
| Parquet | `LOAD_PARQUET` | Columnar format, efficient for large data |

## Performance Tips

1. **Use appropriate data types** - Integer operations are faster than float
2. **Filter early** - Apply filters before expensive operations
3. **Enable optimizations** - Use `-O` flag when compiling
4. **Prefer vector operations** - Use VEC_* ops instead of scalar loops
5. **Use Parquet for large files** - Columnar format is more efficient

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
