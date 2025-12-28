package dsl

// Node is the interface implemented by all AST nodes.
type Node interface {
	node()
}

// Expr is the interface implemented by all expression nodes.
type Expr interface {
	Node
	expr()
}

// Stmt is the interface implemented by all statement nodes.
type Stmt interface {
	Node
	stmt()
}

// Program represents a complete DSL program.
type Program struct {
	Statements []Stmt
}

func (*Program) node() {}

// ===== Statements =====

// AssignStmt represents a variable assignment.
// Example: data = load("sales.csv")
type AssignStmt struct {
	Name  string
	Value Expr
}

func (*AssignStmt) node() {}
func (*AssignStmt) stmt() {}

// ReturnStmt represents a return statement.
// Example: return sum(total)
type ReturnStmt struct {
	Value Expr
}

func (*ReturnStmt) node() {}
func (*ReturnStmt) stmt() {}

// ExprStmt represents an expression used as a statement.
type ExprStmt struct {
	Expr Expr
}

func (*ExprStmt) node() {}
func (*ExprStmt) stmt() {}

// ===== Expressions =====

// Ident represents an identifier.
type Ident struct {
	Name string
}

func (*Ident) node() {}
func (*Ident) expr() {}

// IntLit represents an integer literal.
type IntLit struct {
	Value int64
}

func (*IntLit) node() {}
func (*IntLit) expr() {}

// FloatLit represents a float literal.
type FloatLit struct {
	Value float64
}

func (*FloatLit) node() {}
func (*FloatLit) expr() {}

// StringLit represents a string literal.
type StringLit struct {
	Value string
}

func (*StringLit) node() {}
func (*StringLit) expr() {}

// BoolLit represents a boolean literal.
type BoolLit struct {
	Value bool
}

func (*BoolLit) node() {}
func (*BoolLit) expr() {}

// BinaryExpr represents a binary expression.
// Example: price * quantity, x > 10
type BinaryExpr struct {
	Left  Expr
	Op    TokenType
	Right Expr
}

func (*BinaryExpr) node() {}
func (*BinaryExpr) expr() {}

// UnaryExpr represents a unary expression.
// Example: not x, -value
type UnaryExpr struct {
	Op    TokenType
	Right Expr
}

func (*UnaryExpr) node() {}
func (*UnaryExpr) expr() {}

// CallExpr represents a function call.
// Example: sum(price), load("data.csv")
type CallExpr struct {
	Func string
	Args []Expr
}

func (*CallExpr) node() {}
func (*CallExpr) expr() {}

// PipeExpr represents a pipe expression.
// Example: data |> filter(x > 10) |> select(x, y)
type PipeExpr struct {
	Left  Expr
	Right Expr
}

func (*PipeExpr) node() {}
func (*PipeExpr) expr() {}

// MemberExpr represents a member access.
// Example: data.price, result.total
type MemberExpr struct {
	Object Expr
	Member string
}

func (*MemberExpr) node() {}
func (*MemberExpr) expr() {}

// IndexExpr represents an index access.
// Example: data[0], columns["price"]
type IndexExpr struct {
	Object Expr
	Index  Expr
}

func (*IndexExpr) node() {}
func (*IndexExpr) expr() {}

// ===== DataFrame Operations =====

// LoadExpr represents loading data from a file.
// Example: load("sales.csv")
type LoadExpr struct {
	Path string
}

func (*LoadExpr) node() {}
func (*LoadExpr) expr() {}

// FrameExpr represents loading a predeclared frame.
// Example: frame("data")
type FrameExpr struct {
	Name string
}

func (*FrameExpr) node() {}
func (*FrameExpr) expr() {}

// SelectExpr represents column selection.
// Example: select(price, quantity, total)
type SelectExpr struct {
	Columns []Expr
}

func (*SelectExpr) node() {}
func (*SelectExpr) expr() {}

// FilterExpr represents row filtering.
// Example: filter(quantity > 10)
type FilterExpr struct {
	Condition Expr
}

func (*FilterExpr) node() {}
func (*FilterExpr) expr() {}

// MutateExpr represents computed column creation.
// Example: mutate(total = price * quantity)
type MutateExpr struct {
	Assignments []MutateAssign
}

type MutateAssign struct {
	Name  string
	Value Expr
}

func (*MutateExpr) node() {}
func (*MutateExpr) expr() {}

// GroupByExpr represents grouping by columns.
// Example: group_by(category)
type GroupByExpr struct {
	Keys []string
}

func (*GroupByExpr) node() {}
func (*GroupByExpr) expr() {}

// SummarizeExpr represents aggregation after grouping.
// Example: summarize(total = sum(price), n = count())
type SummarizeExpr struct {
	Aggregations []AggregateAssign
}

type AggregateAssign struct {
	Name string
	Func string
	Args []Expr
}

func (*SummarizeExpr) node() {}
func (*SummarizeExpr) expr() {}

// JoinExpr represents a join operation.
// Example: join(other, on: id) or left_join(other, on: id)
type JoinExpr struct {
	JoinType string // "inner", "left", "right"
	Right    Expr
	On       string // join key column name
}

func (*JoinExpr) node() {}
func (*JoinExpr) expr() {}

// LoadJSONExpr represents loading data from a JSON file.
// Example: load_json("data.json")
type LoadJSONExpr struct {
	Path string
}

func (*LoadJSONExpr) node() {}
func (*LoadJSONExpr) expr() {}

// LoadParquetExpr represents loading data from a Parquet file.
// Example: load_parquet("data.parquet")
type LoadParquetExpr struct {
	Path string
}

func (*LoadParquetExpr) node() {}
func (*LoadParquetExpr) expr() {}

// NewFrameExpr represents creating an empty frame.
// Example: new_frame()
type NewFrameExpr struct{}

func (*NewFrameExpr) node() {}
func (*NewFrameExpr) expr() {}

// TakeExpr represents taking rows by indices.
// Example: take(10) or take(indices)
type TakeExpr struct {
	Count   Expr   // Number of rows or index vector
	Indices []Expr // Optional list of indices
}

func (*TakeExpr) node() {}
func (*TakeExpr) expr() {}
