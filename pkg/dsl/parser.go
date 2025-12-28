package dsl

import (
	"fmt"
	"strconv"
)

// Parser parses DSL tokens into an AST.
type Parser struct {
	tokens []Token
	pos    int
	errors []error
}

// NewParser creates a new parser for the given tokens.
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
		errors: []error{},
	}
}

// Parse parses the tokens into a Program AST.
func (p *Parser) Parse() (*Program, error) {
	program := &Program{
		Statements: []Stmt{},
	}

	for !p.isAtEnd() {
		p.skipNewlines()
		if p.isAtEnd() {
			break
		}

		stmt := p.parseStatement()
		if stmt != nil {
			program.Statements = append(program.Statements, stmt)
		}
	}

	if len(p.errors) > 0 {
		return nil, p.errors[0]
	}

	return program, nil
}

func (p *Parser) parseStatement() Stmt {
	// Check for return statement
	if p.check(TokenReturn) {
		return p.parseReturnStmt()
	}

	// Check for assignment: ident = expr
	if p.check(TokenIdent) && p.peekNext().Type == TokenAssign {
		return p.parseAssignStmt()
	}

	// Otherwise, parse as expression statement
	expr := p.parseExpression()
	if expr != nil {
		return &ExprStmt{Expr: expr}
	}

	return nil
}

func (p *Parser) parseReturnStmt() *ReturnStmt {
	p.advance() // consume 'return'
	expr := p.parseExpression()
	return &ReturnStmt{Value: expr}
}

func (p *Parser) parseAssignStmt() *AssignStmt {
	name := p.advance().Value
	p.advance() // consume '='
	expr := p.parseExpression()
	return &AssignStmt{Name: name, Value: expr}
}

func (p *Parser) parseExpression() Expr {
	return p.parsePipe()
}

func (p *Parser) parsePipe() Expr {
	left := p.parseOr()

	for p.check(TokenPipe) {
		p.advance() // consume '|>'
		right := p.parsePipeOperation()
		left = &PipeExpr{Left: left, Right: right}
	}

	return left
}

func (p *Parser) parsePipeOperation() Expr {
	// Parse pipe operations: filter, select, mutate, group_by, summarize, join
	switch {
	case p.check(TokenFilter):
		return p.parseFilter()
	case p.check(TokenSelect):
		return p.parseSelect()
	case p.check(TokenMutate):
		return p.parseMutate()
	case p.check(TokenGroupBy):
		return p.parseGroupBy()
	case p.check(TokenSummarize):
		return p.parseSummarize()
	case p.check(TokenJoin):
		return p.parseJoin("inner")
	case p.check(TokenLeftJoin):
		return p.parseJoin("left")
	case p.check(TokenRightJoin):
		return p.parseJoin("right")
	case p.check(TokenOuterJoin):
		return p.parseJoin("outer")
	case p.check(TokenTake):
		return p.parseTake()
	default:
		return p.parseOr()
	}
}

func (p *Parser) parseTake() Expr {
	p.advance() // consume 'take'
	p.expect(TokenLParen)
	count := p.parseExpression()
	p.expect(TokenRParen)
	return &TakeExpr{Count: count}
}

func (p *Parser) parseFilter() Expr {
	p.advance() // consume 'filter'
	p.expect(TokenLParen)
	condition := p.parseExpression()
	p.expect(TokenRParen)
	return &FilterExpr{Condition: condition}
}

func (p *Parser) parseSelect() Expr {
	p.advance() // consume 'select'
	p.expect(TokenLParen)

	columns := []Expr{}
	for !p.check(TokenRParen) && !p.isAtEnd() {
		col := p.parseExpression()
		columns = append(columns, col)

		if !p.check(TokenComma) {
			break
		}
		p.advance() // consume ','
	}

	p.expect(TokenRParen)
	return &SelectExpr{Columns: columns}
}

func (p *Parser) parseMutate() Expr {
	p.advance() // consume 'mutate'
	p.expect(TokenLParen)

	assignments := []MutateAssign{}
	for !p.check(TokenRParen) && !p.isAtEnd() {
		name := p.expect(TokenIdent).Value
		p.expect(TokenAssign)
		value := p.parseExpression()
		assignments = append(assignments, MutateAssign{Name: name, Value: value})

		if !p.check(TokenComma) {
			break
		}
		p.advance() // consume ','
	}

	p.expect(TokenRParen)
	return &MutateExpr{Assignments: assignments}
}

func (p *Parser) parseGroupBy() Expr {
	p.advance() // consume 'group_by'
	p.expect(TokenLParen)

	keys := []string{}
	for !p.check(TokenRParen) && !p.isAtEnd() {
		key := p.expect(TokenIdent).Value
		keys = append(keys, key)

		if !p.check(TokenComma) {
			break
		}
		p.advance() // consume ','
	}

	p.expect(TokenRParen)
	return &GroupByExpr{Keys: keys}
}

func (p *Parser) parseSummarize() Expr {
	p.advance() // consume 'summarize'
	p.expect(TokenLParen)

	aggregations := []AggregateAssign{}
	for !p.check(TokenRParen) && !p.isAtEnd() {
		nameTok := p.peek()
		switch nameTok.Type {
		case TokenIdent, TokenSum, TokenCount, TokenMean, TokenMin, TokenMax:
			nameTok = p.advance()
		default:
			nameTok = p.expect(TokenIdent)
		}
		name := nameTok.Value
		p.expect(TokenAssign)

		// Parse aggregation function
		funcName := p.advance().Value
		p.expect(TokenLParen)

		args := []Expr{}
		for !p.check(TokenRParen) && !p.isAtEnd() {
			arg := p.parseExpression()
			args = append(args, arg)

			if !p.check(TokenComma) {
				break
			}
			p.advance()
		}
		p.expect(TokenRParen)

		aggregations = append(aggregations, AggregateAssign{
			Name: name,
			Func: funcName,
			Args: args,
		})

		if !p.check(TokenComma) {
			break
		}
		p.advance() // consume ','
	}

	p.expect(TokenRParen)
	return &SummarizeExpr{Aggregations: aggregations}
}

func (p *Parser) parseJoin(joinType string) Expr {
	p.advance() // consume 'join', 'left_join', or 'right_join'
	p.expect(TokenLParen)

	right := p.parseExpression()

	// Parse 'on' keyword
	var onColumn string
	if p.check(TokenComma) {
		p.advance()
		// Expect 'on' or 'on:'
		if p.check(TokenIdent) && p.peek().Value == "on" {
			p.advance()
			if p.check(TokenColon) {
				p.advance()
			} else if p.check(TokenAssign) {
				p.advance()
			}
			onColumn = p.expect(TokenIdent).Value
		}
	}

	p.expect(TokenRParen)
	return &JoinExpr{JoinType: joinType, Right: right, On: onColumn}
}

func (p *Parser) parseOr() Expr {
	left := p.parseAnd()

	for p.check(TokenOr) {
		p.advance()
		right := p.parseAnd()
		left = &BinaryExpr{Left: left, Op: TokenOr, Right: right}
	}

	return left
}

func (p *Parser) parseAnd() Expr {
	left := p.parseEquality()

	for p.check(TokenAnd) {
		p.advance()
		right := p.parseEquality()
		left = &BinaryExpr{Left: left, Op: TokenAnd, Right: right}
	}

	return left
}

func (p *Parser) parseEquality() Expr {
	left := p.parseComparison()

	for p.check(TokenEQ) || p.check(TokenNE) {
		op := p.advance().Type
		right := p.parseComparison()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseComparison() Expr {
	left := p.parseAdditive()

	for p.check(TokenLT) || p.check(TokenLE) || p.check(TokenGT) || p.check(TokenGE) {
		op := p.advance().Type
		right := p.parseAdditive()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseAdditive() Expr {
	left := p.parseMultiplicative()

	for p.check(TokenPlus) || p.check(TokenMinus) {
		op := p.advance().Type
		right := p.parseMultiplicative()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseMultiplicative() Expr {
	left := p.parseUnary()

	for p.check(TokenStar) || p.check(TokenSlash) || p.check(TokenPercent) {
		op := p.advance().Type
		right := p.parseUnary()
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

func (p *Parser) parseUnary() Expr {
	if p.check(TokenNot) || p.check(TokenMinus) {
		op := p.advance().Type
		right := p.parseUnary()
		return &UnaryExpr{Op: op, Right: right}
	}

	return p.parsePostfix()
}

func (p *Parser) parsePostfix() Expr {
	expr := p.parsePrimary()

	for {
		if p.check(TokenDot) {
			p.advance()
			member := p.expect(TokenIdent).Value
			expr = &MemberExpr{Object: expr, Member: member}
		} else if p.check(TokenLBracket) {
			p.advance()
			index := p.parseExpression()
			p.expect(TokenRBracket)
			expr = &IndexExpr{Object: expr, Index: index}
		} else if p.check(TokenLParen) {
			// Function call on identifier
			if ident, ok := expr.(*Ident); ok {
				expr = p.parseCall(ident.Name)
			} else {
				break
			}
		} else {
			break
		}
	}

	return expr
}

func (p *Parser) parseCall(name string) Expr {
	p.expect(TokenLParen)

	args := []Expr{}
	for !p.check(TokenRParen) && !p.isAtEnd() {
		arg := p.parseExpression()
		args = append(args, arg)

		if !p.check(TokenComma) {
			break
		}
		p.advance()
	}

	p.expect(TokenRParen)

	// Check for special functions
	switch name {
	case "load":
		if len(args) == 1 {
			if str, ok := args[0].(*StringLit); ok {
				return &LoadExpr{Path: str.Value}
			}
		}
	case "frame":
		if len(args) == 1 {
			if str, ok := args[0].(*StringLit); ok {
				return &FrameExpr{Name: str.Value}
			}
		}
	case "load_json":
		if len(args) == 1 {
			if str, ok := args[0].(*StringLit); ok {
				return &LoadJSONExpr{Path: str.Value}
			}
		}
	case "load_parquet":
		if len(args) == 1 {
			if str, ok := args[0].(*StringLit); ok {
				return &LoadParquetExpr{Path: str.Value}
			}
		}
	case "new_frame":
		return &NewFrameExpr{}
	}

	return &CallExpr{Func: name, Args: args}
}

func (p *Parser) parsePrimary() Expr {
	switch {
	case p.check(TokenInt):
		val, _ := strconv.ParseInt(p.advance().Value, 10, 64)
		return &IntLit{Value: val}

	case p.check(TokenFloat):
		val, _ := strconv.ParseFloat(p.advance().Value, 64)
		return &FloatLit{Value: val}

	case p.check(TokenString):
		return &StringLit{Value: p.advance().Value}

	case p.check(TokenTrue):
		p.advance()
		return &BoolLit{Value: true}

	case p.check(TokenFalse):
		p.advance()
		return &BoolLit{Value: false}

	case p.check(TokenLoad):
		p.advance()
		return p.parseCall("load")

	case p.check(TokenFrame):
		p.advance()
		return p.parseCall("frame")

	case p.check(TokenIdent):
		return &Ident{Name: p.advance().Value}

	case p.check(TokenLParen):
		p.advance()
		expr := p.parseExpression()
		p.expect(TokenRParen)
		return expr

	// Aggregation functions
	case p.check(TokenSum), p.check(TokenCount), p.check(TokenMean),
		p.check(TokenMin), p.check(TokenMax):
		name := p.advance().Value
		return p.parseCall(name)

	// String functions
	case p.check(TokenUpper), p.check(TokenLower), p.check(TokenTrim),
		p.check(TokenContains), p.check(TokenStartsWith), p.check(TokenEndsWith),
		p.check(TokenConcat), p.check(TokenLen):
		name := p.advance().Value
		return p.parseCall(name)

	// DataFrame operations as standalone functions
	case p.check(TokenGroupBy):
		return p.parseGroupBy()

	case p.check(TokenFilter):
		return p.parseFilter()

	case p.check(TokenSelect):
		return p.parseSelect()

	case p.check(TokenMutate):
		return p.parseMutate()

	case p.check(TokenSummarize):
		return p.parseSummarize()

	case p.check(TokenJoin):
		return p.parseJoin("inner")

	case p.check(TokenLeftJoin):
		return p.parseJoin("left")

	case p.check(TokenRightJoin):
		return p.parseJoin("right")

	case p.check(TokenOuterJoin):
		return p.parseJoin("outer")

	case p.check(TokenLoadJSON):
		p.advance()
		return p.parseCall("load_json")

	case p.check(TokenLoadParquet):
		p.advance()
		return p.parseCall("load_parquet")

	case p.check(TokenNewFrame):
		p.advance()
		return p.parseCall("new_frame")

	case p.check(TokenTake):
		return p.parseTake()

	case p.check(TokenRowCount), p.check(TokenColCount),
		p.check(TokenSplit), p.check(TokenReplace), p.check(TokenAddCol):
		name := p.advance().Value
		return p.parseCall(name)

	default:
		p.error(fmt.Sprintf("unexpected token: %v", p.peek()))
		return nil
	}
}

// Helper methods

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) peekNext() Token {
	if p.pos+1 >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos+1]
}

func (p *Parser) advance() Token {
	if !p.isAtEnd() {
		p.pos++
	}
	return p.tokens[p.pos-1]
}

func (p *Parser) check(t TokenType) bool {
	return p.peek().Type == t
}

func (p *Parser) expect(t TokenType) Token {
	if p.check(t) {
		return p.advance()
	}
	p.error(fmt.Sprintf("expected %v, got %v", t, p.peek().Type))
	return Token{}
}

func (p *Parser) isAtEnd() bool {
	return p.peek().Type == TokenEOF
}

func (p *Parser) skipNewlines() {
	for p.check(TokenNewline) {
		p.advance()
	}
}

func (p *Parser) error(msg string) {
	tok := p.peek()
	err := fmt.Errorf("line %d, col %d: %s", tok.Line, tok.Col, msg)
	p.errors = append(p.errors, err)
	p.advance() // Skip problematic token
}
