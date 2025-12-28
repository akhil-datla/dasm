package dsl

import (
	"fmt"
	"strings"
)

// Compiler compiles DSL AST to DFL assembly code.
type Compiler struct {
	output     strings.Builder
	constants  []string
	nextReg    int // Next available R register
	nextVReg   int // Next available V register
	nextFReg   int // Next available F register
	variables  map[string]regInfo
	masks      map[int]regInfo // Maps frame register to its filter mask
	groupByReg int             // Register holding current groupby result
}

type regInfo struct {
	regType string // "R", "V", or "F"
	regNum  int
}

// NewCompiler creates a new DSL compiler.
func NewCompiler() *Compiler {
	return &Compiler{
		nextReg:    0,
		nextVReg:   0,
		nextFReg:   0,
		variables:  make(map[string]regInfo),
		masks:      make(map[int]regInfo),
		groupByReg: -1,
	}
}

// Compile compiles a DSL program to assembly code.
func (c *Compiler) Compile(program *Program) (string, error) {
	for _, stmt := range program.Statements {
		if err := c.compileStmt(stmt); err != nil {
			return "", err
		}
	}

	return c.output.String(), nil
}

func (c *Compiler) compileStmt(stmt Stmt) error {
	switch s := stmt.(type) {
	case *AssignStmt:
		return c.compileAssign(s)
	case *ReturnStmt:
		return c.compileReturn(s)
	case *ExprStmt:
		_, err := c.compileExpr(s.Expr)
		return err
	default:
		return fmt.Errorf("unknown statement type: %T", stmt)
	}
}

func (c *Compiler) compileAssign(stmt *AssignStmt) error {
	reg, err := c.compileExpr(stmt.Value)
	if err != nil {
		return err
	}
	c.variables[stmt.Name] = reg
	return nil
}

func (c *Compiler) compileReturn(stmt *ReturnStmt) error {
	reg, err := c.compileExpr(stmt.Value)
	if err != nil {
		return err
	}

	switch reg.regType {
	case "R":
		c.emit("HALT          R%d", reg.regNum)
	case "F":
		c.emit("HALT_F        F%d", reg.regNum)
	case "V":
		// Return the vector directly using HALT_V
		c.emit("HALT_V        V%d", reg.regNum)
	}

	return nil
}

func (c *Compiler) compileExpr(expr Expr) (regInfo, error) {
	switch e := expr.(type) {
	case *IntLit:
		return c.compileIntLit(e)
	case *FloatLit:
		return c.compileFloatLit(e)
	case *StringLit:
		return c.compileStringLit(e)
	case *BoolLit:
		return c.compileBoolLit(e)
	case *Ident:
		return c.compileIdent(e)
	case *BinaryExpr:
		return c.compileBinary(e)
	case *UnaryExpr:
		return c.compileUnary(e)
	case *LoadExpr:
		return c.compileLoad(e)
	case *FrameExpr:
		return c.compileFrame(e)
	case *PipeExpr:
		return c.compilePipe(e)
	case *CallExpr:
		return c.compileCall(e)
	case *MemberExpr:
		return c.compileMember(e)
	case *FilterExpr:
		return c.compileFilter(e, regInfo{})
	case *SelectExpr:
		return c.compileSelect(e, regInfo{})
	case *MutateExpr:
		return c.compileMutate(e, regInfo{})
	case *GroupByExpr:
		return c.compileGroupBy(e, regInfo{})
	case *SummarizeExpr:
		return c.compileSummarize(e, regInfo{})
	case *JoinExpr:
		return c.compileJoin(e, regInfo{})
	case *LoadJSONExpr:
		return c.compileLoadJSON(e)
	case *LoadParquetExpr:
		return c.compileLoadParquet(e)
	case *NewFrameExpr:
		return c.compileNewFrame(e)
	case *TakeExpr:
		return c.compileTake(e, regInfo{})
	default:
		return regInfo{}, fmt.Errorf("unknown expression type: %T", expr)
	}
}

func (c *Compiler) compileIntLit(e *IntLit) (regInfo, error) {
	reg := c.allocReg()
	c.emit("LOAD_CONST    R%d, %d", reg, e.Value)
	return regInfo{"R", reg}, nil
}

func (c *Compiler) compileFloatLit(e *FloatLit) (regInfo, error) {
	reg := c.allocFReg()
	c.emit("LOAD_CONST_F  F%d, %g", reg, e.Value)
	return regInfo{"F", reg}, nil
}

func (c *Compiler) compileStringLit(e *StringLit) (regInfo, error) {
	// Strings are typically used as constants
	return regInfo{}, nil
}

func (c *Compiler) compileBoolLit(e *BoolLit) (regInfo, error) {
	reg := c.allocReg()
	val := 0
	if e.Value {
		val = 1
	}
	c.emit("LOAD_CONST    R%d, %d", reg, val)
	return regInfo{"R", reg}, nil
}

func (c *Compiler) compileIdent(e *Ident) (regInfo, error) {
	if info, ok := c.variables[e.Name]; ok {
		return info, nil
	}
	// Assume it's a column reference - we'll need context to resolve it
	return regInfo{}, fmt.Errorf("undefined variable: %s", e.Name)
}

func (c *Compiler) compileBinary(e *BinaryExpr) (regInfo, error) {
	left, err := c.compileExpr(e.Left)
	if err != nil {
		return regInfo{}, err
	}

	right, err := c.compileExpr(e.Right)
	if err != nil {
		return regInfo{}, err
	}

	// Handle vector operations
	if left.regType == "V" || right.regType == "V" {
		return c.compileVectorBinary(e.Op, left, right)
	}

	// Handle scalar operations
	return c.compileScalarBinary(e.Op, left, right)
}

func (c *Compiler) compileVectorBinary(op TokenType, left, right regInfo) (regInfo, error) {
	dst := c.allocVReg()

	// Ensure both are vectors (broadcast if needed)
	if left.regType != "V" {
		broadcastDst := c.allocVReg()
		c.emit("BROADCAST     V%d, R%d, V%d", broadcastDst, left.regNum, right.regNum)
		left = regInfo{"V", broadcastDst}
	}
	if right.regType != "V" {
		broadcastDst := c.allocVReg()
		c.emit("BROADCAST     V%d, R%d, V%d", broadcastDst, right.regNum, left.regNum)
		right = regInfo{"V", broadcastDst}
	}

	switch op {
	case TokenPlus:
		c.emit("VEC_ADD_F     V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenMinus:
		c.emit("VEC_SUB_F     V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenStar:
		c.emit("VEC_MUL_F     V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenSlash:
		c.emit("VEC_DIV_F     V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenPercent:
		c.emit("VEC_MOD       V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenLT:
		c.emit("CMP_LT        V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenLE:
		c.emit("CMP_LE        V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenGT:
		c.emit("CMP_GT        V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenGE:
		c.emit("CMP_GE        V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenEQ:
		c.emit("CMP_EQ        V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenNE:
		c.emit("CMP_NE        V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenAnd:
		c.emit("AND           V%d, V%d, V%d", dst, left.regNum, right.regNum)
	case TokenOr:
		c.emit("OR            V%d, V%d, V%d", dst, left.regNum, right.regNum)
	default:
		return regInfo{}, fmt.Errorf("unsupported vector operation: %v", op)
	}

	return regInfo{"V", dst}, nil
}

func (c *Compiler) compileScalarBinary(op TokenType, left, right regInfo) (regInfo, error) {
	dst := c.allocReg()

	switch op {
	case TokenPlus:
		c.emit("ADD_R         R%d, R%d, R%d", dst, left.regNum, right.regNum)
	case TokenMinus:
		c.emit("SUB_R         R%d, R%d, R%d", dst, left.regNum, right.regNum)
	case TokenStar:
		c.emit("MUL_R         R%d, R%d, R%d", dst, left.regNum, right.regNum)
	case TokenSlash:
		c.emit("DIV_R         R%d, R%d, R%d", dst, left.regNum, right.regNum)
	default:
		return regInfo{}, fmt.Errorf("unsupported scalar operation: %v", op)
	}

	return regInfo{"R", dst}, nil
}

func (c *Compiler) compileUnary(e *UnaryExpr) (regInfo, error) {
	right, err := c.compileExpr(e.Right)
	if err != nil {
		return regInfo{}, err
	}

	if e.Op == TokenNot && right.regType == "V" {
		dst := c.allocVReg()
		c.emit("NOT           V%d, V%d", dst, right.regNum)
		return regInfo{"V", dst}, nil
	}

	return regInfo{}, fmt.Errorf("unsupported unary operation")
}

func (c *Compiler) compileLoad(e *LoadExpr) (regInfo, error) {
	reg := c.allocReg()
	constIdx := c.addConstant(fmt.Sprintf("\"%s\"", e.Path))
	c.emit("LOAD_CSV      R%d, \"%s\"", reg, e.Path)
	_ = constIdx
	return regInfo{"R", reg}, nil
}

func (c *Compiler) compileFrame(e *FrameExpr) (regInfo, error) {
	reg := c.allocReg()
	c.emit("LOAD_FRAME    R%d, \"%s\"", reg, e.Name)
	return regInfo{"R", reg}, nil
}

func (c *Compiler) compilePipe(e *PipeExpr) (regInfo, error) {
	// Compile left side first
	left, err := c.compileExpr(e.Left)
	if err != nil {
		return regInfo{}, err
	}

	// Compile right side with context from left
	return c.compilePipeRight(e.Right, left)
}

func (c *Compiler) compilePipeRight(expr Expr, input regInfo) (regInfo, error) {
	switch e := expr.(type) {
	case *FilterExpr:
		return c.compileFilter(e, input)
	case *SelectExpr:
		return c.compileSelect(e, input)
	case *MutateExpr:
		return c.compileMutate(e, input)
	case *GroupByExpr:
		return c.compileGroupBy(e, input)
	case *SummarizeExpr:
		return c.compileSummarize(e, input)
	case *JoinExpr:
		return c.compileJoin(e, input)
	case *TakeExpr:
		return c.compileTake(e, input)
	case *CallExpr:
		return c.compileCallWithInput(e, input)
	default:
		return c.compileExpr(expr)
	}
}

func (c *Compiler) compileFilter(e *FilterExpr, input regInfo) (regInfo, error) {
	// The input should be a frame (R register)
	// The condition should produce a boolean vector

	// Compile condition in context of input frame
	mask, err := c.compileCondition(e.Condition, input)
	if err != nil {
		return regInfo{}, err
	}

	// Store the mask associated with this frame's register
	// So when columns are selected from this frame, they get filtered
	if input.regType == "R" {
		c.masks[input.regNum] = mask
	}
	c.variables["_mask"] = mask

	return input, nil
}

func (c *Compiler) compileCondition(expr Expr, frame regInfo) (regInfo, error) {
	switch e := expr.(type) {
	case *BinaryExpr:
		return c.compileConditionBinary(e, frame)
	case *Ident:
		// Column reference
		vReg := c.allocVReg()
		c.emit("SELECT_COL    V%d, R%d, \"%s\"", vReg, frame.regNum, e.Name)
		return regInfo{"V", vReg}, nil
	case *IntLit:
		return c.compileIntLit(e)
	case *FloatLit:
		return c.compileFloatLit(e)
	default:
		return c.compileExpr(expr)
	}
}

func (c *Compiler) compileConditionBinary(e *BinaryExpr, frame regInfo) (regInfo, error) {
	left, err := c.compileCondition(e.Left, frame)
	if err != nil {
		return regInfo{}, err
	}

	right, err := c.compileCondition(e.Right, frame)
	if err != nil {
		return regInfo{}, err
	}

	return c.compileVectorBinary(e.Op, left, right)
}

func (c *Compiler) compileSelect(e *SelectExpr, input regInfo) (regInfo, error) {
	// For select, we just store which columns are selected
	// The actual selection happens when we need the columns
	for _, col := range e.Columns {
		if ident, ok := col.(*Ident); ok {
			vReg := c.allocVReg()
			c.emit("SELECT_COL    V%d, R%d, \"%s\"", vReg, input.regNum, ident.Name)
			c.variables[ident.Name] = regInfo{"V", vReg}
		}
	}
	return input, nil
}

func (c *Compiler) compileMutate(e *MutateExpr, input regInfo) (regInfo, error) {
	for _, assign := range e.Assignments {
		// Compile the expression
		val, err := c.compileExprWithFrame(assign.Value, input)
		if err != nil {
			return regInfo{}, err
		}
		c.variables[assign.Name] = val
	}
	return input, nil
}

func (c *Compiler) compileExprWithFrame(expr Expr, frame regInfo) (regInfo, error) {
	switch e := expr.(type) {
	case *Ident:
		// Check if it's a known variable
		if info, ok := c.variables[e.Name]; ok {
			return info, nil
		}
		// Otherwise, select from frame
		vReg := c.allocVReg()
		c.emit("SELECT_COL    V%d, R%d, \"%s\"", vReg, frame.regNum, e.Name)
		return regInfo{"V", vReg}, nil
	case *BinaryExpr:
		left, err := c.compileExprWithFrame(e.Left, frame)
		if err != nil {
			return regInfo{}, err
		}
		right, err := c.compileExprWithFrame(e.Right, frame)
		if err != nil {
			return regInfo{}, err
		}
		if left.regType == "V" || right.regType == "V" {
			return c.compileVectorBinary(e.Op, left, right)
		}
		return c.compileScalarBinary(e.Op, left, right)
	default:
		return c.compileExpr(expr)
	}
}

func (c *Compiler) compileGroupBy(e *GroupByExpr, input regInfo) (regInfo, error) {
	if len(e.Keys) == 0 {
		return input, nil
	}

	// Select the key column
	keyCol := e.Keys[0]
	keyVReg := c.allocVReg()
	c.emit("SELECT_COL    V%d, R%d, \"%s\"", keyVReg, input.regNum, keyCol)

	// Create groupby result
	gbReg := c.allocReg()
	c.emit("GROUP_BY      R%d, V%d", gbReg, keyVReg)
	c.groupByReg = gbReg

	return regInfo{"R", gbReg}, nil
}

func (c *Compiler) compileSummarize(e *SummarizeExpr, input regInfo) (regInfo, error) {
	if c.groupByReg < 0 {
		return regInfo{}, fmt.Errorf("summarize requires group_by")
	}

	for _, agg := range e.Aggregations {
		switch strings.ToLower(agg.Func) {
		case "count":
			vReg := c.allocVReg()
			c.emit("GROUP_COUNT   V%d, R%d", vReg, c.groupByReg)
			c.variables[agg.Name] = regInfo{"V", vReg}

		case "sum":
			if len(agg.Args) > 0 {
				// Get column to sum
				colInfo, err := c.compileExpr(agg.Args[0])
				if err != nil {
					return regInfo{}, err
				}
				vReg := c.allocVReg()
				c.emit("GROUP_SUM     V%d, R%d, V%d", vReg, c.groupByReg, colInfo.regNum)
				c.variables[agg.Name] = regInfo{"V", vReg}
			}

		case "mean", "avg":
			if len(agg.Args) > 0 {
				colInfo, err := c.compileExpr(agg.Args[0])
				if err != nil {
					return regInfo{}, err
				}
				vReg := c.allocVReg()
				c.emit("GROUP_MEAN    V%d, R%d, V%d", vReg, c.groupByReg, colInfo.regNum)
				c.variables[agg.Name] = regInfo{"V", vReg}
			}

		case "min":
			if len(agg.Args) > 0 {
				colInfo, err := c.compileExpr(agg.Args[0])
				if err != nil {
					return regInfo{}, err
				}
				vReg := c.allocVReg()
				c.emit("GROUP_MIN     V%d, R%d, V%d", vReg, c.groupByReg, colInfo.regNum)
				c.variables[agg.Name] = regInfo{"V", vReg}
			}

		case "max":
			if len(agg.Args) > 0 {
				colInfo, err := c.compileExpr(agg.Args[0])
				if err != nil {
					return regInfo{}, err
				}
				vReg := c.allocVReg()
				c.emit("GROUP_MAX     V%d, R%d, V%d", vReg, c.groupByReg, colInfo.regNum)
				c.variables[agg.Name] = regInfo{"V", vReg}
			}
		}
	}

	return input, nil
}

func (c *Compiler) compileJoin(e *JoinExpr, input regInfo) (regInfo, error) {
	right, err := c.compileExpr(e.Right)
	if err != nil {
		return regInfo{}, err
	}

	resultReg := c.allocReg()

	switch e.JoinType {
	case "inner":
		c.emit("JOIN_INNER    R%d, R%d, R%d, \"%s\"", resultReg, input.regNum, right.regNum, e.On)
	case "left":
		c.emit("JOIN_LEFT     R%d, R%d, R%d, \"%s\"", resultReg, input.regNum, right.regNum, e.On)
	case "right":
		c.emit("JOIN_RIGHT    R%d, R%d, R%d, \"%s\"", resultReg, input.regNum, right.regNum, e.On)
	case "outer":
		c.emit("JOIN_OUTER    R%d, R%d, R%d, \"%s\"", resultReg, input.regNum, right.regNum, e.On)
	}

	return regInfo{"R", resultReg}, nil
}

func (c *Compiler) compileLoadJSON(e *LoadJSONExpr) (regInfo, error) {
	reg := c.allocReg()
	c.emit("LOAD_JSON     R%d, \"%s\"", reg, e.Path)
	return regInfo{"R", reg}, nil
}

func (c *Compiler) compileLoadParquet(e *LoadParquetExpr) (regInfo, error) {
	reg := c.allocReg()
	c.emit("LOAD_PARQUET  R%d, \"%s\"", reg, e.Path)
	return regInfo{"R", reg}, nil
}

func (c *Compiler) compileNewFrame(e *NewFrameExpr) (regInfo, error) {
	reg := c.allocReg()
	c.emit("NEW_FRAME     R%d", reg)
	return regInfo{"R", reg}, nil
}

func (c *Compiler) compileTake(e *TakeExpr, input regInfo) (regInfo, error) {
	count, err := c.compileExpr(e.Count)
	if err != nil {
		return regInfo{}, err
	}

	if input.regType == "V" {
		// Taking from a vector
		vReg := c.allocVReg()
		if count.regType == "R" {
			c.emit("TAKE          V%d, V%d, R%d", vReg, input.regNum, count.regNum)
		} else if count.regType == "V" {
			c.emit("TAKE          V%d, V%d, V%d", vReg, input.regNum, count.regNum)
		}
		return regInfo{"V", vReg}, nil
	}

	return input, nil
}

func (c *Compiler) compileCall(e *CallExpr) (regInfo, error) {
	switch strings.ToLower(e.Func) {
	case "sum":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				fReg := c.allocFReg()
				c.emit("REDUCE_SUM_F  F%d, V%d", fReg, arg.regNum)
				return regInfo{"F", fReg}, nil
			}
		}

	case "count":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				rReg := c.allocReg()
				c.emit("REDUCE_COUNT  R%d, V%d", rReg, arg.regNum)
				return regInfo{"R", rReg}, nil
			}
		}

	case "mean", "avg":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				fReg := c.allocFReg()
				c.emit("REDUCE_MEAN   F%d, V%d", fReg, arg.regNum)
				return regInfo{"F", fReg}, nil
			}
		}

	case "min":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				fReg := c.allocFReg()
				c.emit("REDUCE_MIN_F  F%d, V%d", fReg, arg.regNum)
				return regInfo{"F", fReg}, nil
			}
		}

	case "max":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				fReg := c.allocFReg()
				c.emit("REDUCE_MAX_F  F%d, V%d", fReg, arg.regNum)
				return regInfo{"F", fReg}, nil
			}
		}

	case "upper":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				vReg := c.allocVReg()
				c.emit("STR_UPPER     V%d, V%d", vReg, arg.regNum)
				return regInfo{"V", vReg}, nil
			}
		}

	case "lower":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				vReg := c.allocVReg()
				c.emit("STR_LOWER     V%d, V%d", vReg, arg.regNum)
				return regInfo{"V", vReg}, nil
			}
		}

	case "trim":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				vReg := c.allocVReg()
				c.emit("STR_TRIM      V%d, V%d", vReg, arg.regNum)
				return regInfo{"V", vReg}, nil
			}
		}

	case "len", "length":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "V" {
				vReg := c.allocVReg()
				c.emit("STR_LEN       V%d, V%d", vReg, arg.regNum)
				return regInfo{"V", vReg}, nil
			}
		}

	case "contains":
		if len(e.Args) >= 2 {
			col, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if col.regType != "V" {
				return regInfo{}, fmt.Errorf("contains requires vector input")
			}
			if str, ok := e.Args[1].(*StringLit); ok {
				c.addConstant(fmt.Sprintf("\"%s\"", str.Value))
				vReg := c.allocVReg()
				c.emit("STR_CONTAINS  V%d, V%d, \"%s\"", vReg, col.regNum, str.Value)
				return regInfo{"V", vReg}, nil
			}
			return regInfo{}, fmt.Errorf("contains requires string literal pattern")
		}

	case "starts_with":
		if len(e.Args) >= 2 {
			col, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if col.regType != "V" {
				return regInfo{}, fmt.Errorf("starts_with requires vector input")
			}
			if str, ok := e.Args[1].(*StringLit); ok {
				vReg := c.allocVReg()
				c.emit("STR_STARTS_WITH V%d, V%d, \"%s\"", vReg, col.regNum, str.Value)
				return regInfo{"V", vReg}, nil
			}
		}

	case "ends_with":
		if len(e.Args) >= 2 {
			col, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if col.regType != "V" {
				return regInfo{}, fmt.Errorf("ends_with requires vector input")
			}
			if str, ok := e.Args[1].(*StringLit); ok {
				vReg := c.allocVReg()
				c.emit("STR_ENDS_WITH V%d, V%d, \"%s\"", vReg, col.regNum, str.Value)
				return regInfo{"V", vReg}, nil
			}
		}

	case "split":
		if len(e.Args) >= 2 {
			col, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if col.regType != "V" {
				return regInfo{}, fmt.Errorf("split requires vector input")
			}
			if str, ok := e.Args[1].(*StringLit); ok {
				vReg := c.allocVReg()
				c.emit("STR_SPLIT     V%d, V%d, \"%s\"", vReg, col.regNum, str.Value)
				return regInfo{"V", vReg}, nil
			}
		}

	case "replace":
		if len(e.Args) >= 3 {
			col, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if col.regType != "V" {
				return regInfo{}, fmt.Errorf("replace requires vector input")
			}
			old, okOld := e.Args[1].(*StringLit)
			new, okNew := e.Args[2].(*StringLit)
			if okOld && okNew {
				vReg := c.allocVReg()
				c.emit("STR_REPLACE   V%d, V%d, \"%s\", \"%s\"", vReg, col.regNum, old.Value, new.Value)
				return regInfo{"V", vReg}, nil
			}
		}

	case "concat":
		if len(e.Args) >= 2 {
			left, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			right, err := c.compileExpr(e.Args[1])
			if err != nil {
				return regInfo{}, err
			}
			if left.regType == "V" && right.regType == "V" {
				vReg := c.allocVReg()
				c.emit("STR_CONCAT    V%d, V%d, V%d", vReg, left.regNum, right.regNum)
				return regInfo{"V", vReg}, nil
			}
		}

	case "row_count":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "R" {
				rReg := c.allocReg()
				c.emit("ROW_COUNT     R%d, R%d", rReg, arg.regNum)
				return regInfo{"R", rReg}, nil
			}
		}

	case "col_count":
		if len(e.Args) > 0 {
			arg, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if arg.regType == "R" {
				rReg := c.allocReg()
				c.emit("COL_COUNT     R%d, R%d", rReg, arg.regNum)
				return regInfo{"R", rReg}, nil
			}
		}

	case "add_col":
		if len(e.Args) >= 3 {
			frame, err := c.compileExpr(e.Args[0])
			if err != nil {
				return regInfo{}, err
			}
			if frame.regType != "R" {
				return regInfo{}, fmt.Errorf("add_col requires frame as first argument")
			}
			name, ok := e.Args[1].(*StringLit)
			if !ok {
				return regInfo{}, fmt.Errorf("add_col requires string literal column name")
			}
			col, err := c.compileExpr(e.Args[2])
			if err != nil {
				return regInfo{}, err
			}
			if col.regType != "V" {
				return regInfo{}, fmt.Errorf("add_col requires vector as third argument")
			}
			c.emit("ADD_COL       R%d, V%d, \"%s\"", frame.regNum, col.regNum, name.Value)
			return frame, nil
		}
	}

	return regInfo{}, fmt.Errorf("unknown function: %s", e.Func)
}

func (c *Compiler) compileCallWithInput(e *CallExpr, input regInfo) (regInfo, error) {
	// Same as compileCall but with frame context
	return c.compileCall(e)
}

func (c *Compiler) compileMember(e *MemberExpr) (regInfo, error) {
	obj, err := c.compileExpr(e.Object)
	if err != nil {
		return regInfo{}, err
	}

	if obj.regType == "R" {
		// Accessing column from frame
		vReg := c.allocVReg()
		c.emit("SELECT_COL    V%d, R%d, \"%s\"", vReg, obj.regNum, e.Member)

		// Check if this frame has a filter mask applied
		if mask, ok := c.masks[obj.regNum]; ok {
			// Apply the filter to the selected column
			filteredReg := c.allocVReg()
			c.emit("FILTER        V%d, V%d, V%d", filteredReg, vReg, mask.regNum)
			return regInfo{"V", filteredReg}, nil
		}

		return regInfo{"V", vReg}, nil
	}

	return regInfo{}, fmt.Errorf("cannot access member on %s register", obj.regType)
}

// Helper methods

func (c *Compiler) allocReg() int {
	r := c.nextReg
	c.nextReg++
	if c.nextReg > 15 {
		c.nextReg = 0 // Wrap around (in practice should error)
	}
	return r
}

func (c *Compiler) allocVReg() int {
	r := c.nextVReg
	c.nextVReg++
	if c.nextVReg > 7 {
		c.nextVReg = 0
	}
	return r
}

func (c *Compiler) allocFReg() int {
	r := c.nextFReg
	c.nextFReg++
	if c.nextFReg > 15 {
		c.nextFReg = 0
	}
	return r
}

func (c *Compiler) addConstant(val string) int {
	c.constants = append(c.constants, val)
	return len(c.constants) - 1
}

func (c *Compiler) emit(format string, args ...any) {
	c.output.WriteString(fmt.Sprintf(format, args...))
	c.output.WriteString("\n")
}
