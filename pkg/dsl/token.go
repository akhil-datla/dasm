package dsl

import "fmt"

// TokenType represents the type of a DSL token.
type TokenType uint8

const (
	TokenEOF TokenType = iota
	TokenNewline
	TokenIdent   // variable names, function names
	TokenInt     // integer literals
	TokenFloat   // float literals
	TokenString  // "quoted strings"
	TokenComment // # comment

	// Operators
	TokenAssign  // =
	TokenPipe    // |>
	TokenPlus    // +
	TokenMinus   // -
	TokenStar    // *
	TokenSlash   // /
	TokenPercent // %
	TokenEQ      // ==
	TokenNE      // !=
	TokenLT      // <
	TokenLE      // <=
	TokenGT      // >
	TokenGE      // >=
	TokenAnd     // and, &&
	TokenOr      // or, ||
	TokenNot     // not, !
	TokenDot     // .

	// Delimiters
	TokenLParen   // (
	TokenRParen   // )
	TokenLBracket // [
	TokenRBracket // ]
	TokenComma    // ,
	TokenColon    // :
	TokenLBrace   // {
	TokenRBrace   // }

	// Keywords
	TokenLoad      // load
	TokenFrame     // frame
	TokenSelect    // select
	TokenFilter    // filter, where
	TokenMutate    // mutate
	TokenGroupBy   // group_by
	TokenSummarize // summarize
	TokenJoin      // join
	TokenLeftJoin  // left_join
	TokenRightJoin // right_join
	TokenOuterJoin // outer_join
	TokenReturn    // return

	// Aggregation functions
	TokenSum   // sum
	TokenCount // count
	TokenMean  // mean
	TokenMin   // min
	TokenMax   // max

	// String functions
	TokenUpper      // upper
	TokenLower      // lower
	TokenTrim       // trim
	TokenContains   // contains
	TokenStartsWith // starts_with
	TokenEndsWith   // ends_with
	TokenConcat     // concat
	TokenLen        // len
	TokenSplit      // split
	TokenReplace    // replace

	// Frame operations
	TokenNewFrame // new_frame
	TokenAddCol   // add_col
	TokenRowCount // row_count
	TokenColCount // col_count

	// Data loading
	TokenLoadJSON    // load_json
	TokenLoadParquet // load_parquet

	// Index operations
	TokenTake // take

	// Literals
	TokenTrue  // true
	TokenFalse // false
)

// String returns the string representation of a token type.
func (t TokenType) String() string {
	switch t {
	case TokenEOF:
		return "EOF"
	case TokenNewline:
		return "NEWLINE"
	case TokenIdent:
		return "IDENT"
	case TokenInt:
		return "INT"
	case TokenFloat:
		return "FLOAT"
	case TokenString:
		return "STRING"
	case TokenComment:
		return "COMMENT"
	case TokenAssign:
		return "="
	case TokenPipe:
		return "|>"
	case TokenPlus:
		return "+"
	case TokenMinus:
		return "-"
	case TokenStar:
		return "*"
	case TokenSlash:
		return "/"
	case TokenPercent:
		return "%"
	case TokenEQ:
		return "=="
	case TokenNE:
		return "!="
	case TokenLT:
		return "<"
	case TokenLE:
		return "<="
	case TokenGT:
		return ">"
	case TokenGE:
		return ">="
	case TokenAnd:
		return "AND"
	case TokenOr:
		return "OR"
	case TokenNot:
		return "NOT"
	case TokenDot:
		return "."
	case TokenLParen:
		return "("
	case TokenRParen:
		return ")"
	case TokenLBracket:
		return "["
	case TokenRBracket:
		return "]"
	case TokenComma:
		return ","
	case TokenColon:
		return ":"
	case TokenLBrace:
		return "{"
	case TokenRBrace:
		return "}"
	case TokenLoad:
		return "LOAD"
	case TokenFrame:
		return "FRAME"
	case TokenSelect:
		return "SELECT"
	case TokenFilter:
		return "FILTER"
	case TokenMutate:
		return "MUTATE"
	case TokenGroupBy:
		return "GROUP_BY"
	case TokenSummarize:
		return "SUMMARIZE"
	case TokenJoin:
		return "JOIN"
	case TokenLeftJoin:
		return "LEFT_JOIN"
	case TokenRightJoin:
		return "RIGHT_JOIN"
	case TokenOuterJoin:
		return "OUTER_JOIN"
	case TokenReturn:
		return "RETURN"
	case TokenSum:
		return "SUM"
	case TokenCount:
		return "COUNT"
	case TokenMean:
		return "MEAN"
	case TokenMin:
		return "MIN"
	case TokenMax:
		return "MAX"
	case TokenUpper:
		return "UPPER"
	case TokenLower:
		return "LOWER"
	case TokenTrim:
		return "TRIM"
	case TokenContains:
		return "CONTAINS"
	case TokenStartsWith:
		return "STARTS_WITH"
	case TokenEndsWith:
		return "ENDS_WITH"
	case TokenConcat:
		return "CONCAT"
	case TokenLen:
		return "LEN"
	case TokenSplit:
		return "SPLIT"
	case TokenReplace:
		return "REPLACE"
	case TokenNewFrame:
		return "NEW_FRAME"
	case TokenAddCol:
		return "ADD_COL"
	case TokenRowCount:
		return "ROW_COUNT"
	case TokenColCount:
		return "COL_COUNT"
	case TokenLoadJSON:
		return "LOAD_JSON"
	case TokenLoadParquet:
		return "LOAD_PARQUET"
	case TokenTake:
		return "TAKE"
	case TokenTrue:
		return "TRUE"
	case TokenFalse:
		return "FALSE"
	default:
		return "UNKNOWN"
	}
}

// Token represents a lexical token.
type Token struct {
	Type  TokenType
	Value string
	Line  int
	Col   int
}

// String returns a human-readable representation of the token for debugging.
func (t Token) String() string {
	if t.Value != "" {
		return fmt.Sprintf("%s(%s)@%d:%d", t.Type, t.Value, t.Line, t.Col)
	}
	return fmt.Sprintf("%s@%d:%d", t.Type, t.Line, t.Col)
}

// keywords maps keyword strings to token types.
var keywords = map[string]TokenType{
	"load":         TokenLoad,
	"frame":        TokenFrame,
	"select":       TokenSelect,
	"filter":       TokenFilter,
	"where":        TokenFilter, // alias
	"mutate":       TokenMutate,
	"group_by":     TokenGroupBy,
	"groupby":      TokenGroupBy, // alias
	"summarize":    TokenSummarize,
	"summarise":    TokenSummarize, // British spelling
	"join":         TokenJoin,
	"inner_join":   TokenJoin,
	"left_join":    TokenLeftJoin,
	"right_join":   TokenRightJoin,
	"return":       TokenReturn,
	"sum":          TokenSum,
	"count":        TokenCount,
	"mean":         TokenMean,
	"avg":          TokenMean, // alias
	"min":          TokenMin,
	"max":          TokenMax,
	"upper":        TokenUpper,
	"lower":        TokenLower,
	"trim":         TokenTrim,
	"contains":     TokenContains,
	"starts_with":  TokenStartsWith,
	"ends_with":    TokenEndsWith,
	"concat":       TokenConcat,
	"len":          TokenLen,
	"length":       TokenLen, // alias
	"true":         TokenTrue,
	"false":        TokenFalse,
	"and":          TokenAnd,
	"or":           TokenOr,
	"not":          TokenNot,
	"split":        TokenSplit,
	"replace":      TokenReplace,
	"new_frame":    TokenNewFrame,
	"add_col":      TokenAddCol,
	"row_count":    TokenRowCount,
	"col_count":    TokenColCount,
	"load_json":    TokenLoadJSON,
	"load_parquet": TokenLoadParquet,
	"take":         TokenTake,
	"outer_join":   TokenOuterJoin,
}

// LookupIdent returns the token type for an identifier.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TokenIdent
}
