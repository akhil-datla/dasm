package vm

// Opcode represents a VM instruction opcode.
type Opcode uint8

const (
	// ===== Data Loading (0x00-0x0F) =====
	OpLoadCSV     Opcode = 0x00 // R[dst] = load_csv(constants[imm16])
	OpLoadConst   Opcode = 0x01 // R[dst] = constants[imm16]
	OpLoadConstF  Opcode = 0x02 // F[dst] = float_constants[imm16]
	OpSelectCol   Opcode = 0x03 // V[dst] = R[src1].column(constants[imm16])
	OpBroadcast   Opcode = 0x04 // V[dst] = broadcast(R[src1], length from V[src2])
	OpBroadcastF  Opcode = 0x05 // V[dst] = broadcast(F[src1], length from V[src2])
	OpLoadFrame   Opcode = 0x06 // R[dst] = predeclared_frames[constants[imm16]]
	OpLoadJSON    Opcode = 0x07 // R[dst] = load_json(constants[imm16])
	OpLoadParquet Opcode = 0x08 // R[dst] = load_parquet(constants[imm16])

	// ===== Vector Arithmetic (0x10-0x1F) =====
	OpVecAddI Opcode = 0x10 // V[dst] = V[src1] + V[src2] (int64)
	OpVecSubI Opcode = 0x11 // V[dst] = V[src1] - V[src2] (int64)
	OpVecMulI Opcode = 0x12 // V[dst] = V[src1] * V[src2] (int64)
	OpVecDivI Opcode = 0x13 // V[dst] = V[src1] / V[src2] (int64)
	OpVecModI Opcode = 0x14 // V[dst] = V[src1] % V[src2] (int64)
	OpVecAddF Opcode = 0x15 // V[dst] = V[src1] + V[src2] (float64)
	OpVecSubF Opcode = 0x16 // V[dst] = V[src1] - V[src2] (float64)
	OpVecMulF Opcode = 0x17 // V[dst] = V[src1] * V[src2] (float64)
	OpVecDivF Opcode = 0x18 // V[dst] = V[src1] / V[src2] (float64)

	// ===== Comparison (0x20-0x2F) =====
	OpCmpEQ Opcode = 0x20 // V[dst] = V[src1] == V[src2] (bool column)
	OpCmpNE Opcode = 0x21 // V[dst] = V[src1] != V[src2]
	OpCmpLT Opcode = 0x22 // V[dst] = V[src1] < V[src2]
	OpCmpLE Opcode = 0x23 // V[dst] = V[src1] <= V[src2]
	OpCmpGT Opcode = 0x24 // V[dst] = V[src1] > V[src2]
	OpCmpGE Opcode = 0x25 // V[dst] = V[src1] >= V[src2]

	// ===== Logical (0x30-0x3F) =====
	OpAnd Opcode = 0x30 // V[dst] = V[src1] AND V[src2] (bool columns)
	OpOr  Opcode = 0x31 // V[dst] = V[src1] OR V[src2]
	OpNot Opcode = 0x32 // V[dst] = NOT V[src1]

	// ===== Filtering (0x40-0x4F) =====
	OpFilter Opcode = 0x40 // V[dst] = filter(V[src1], V[src2] as bool mask)
	OpTake   Opcode = 0x41 // V[dst] = V[src1][V[src2] as indices]

	// ===== Aggregations (0x50-0x5F) =====
	OpReduceSum   Opcode = 0x50 // R[dst] = sum(V[src1])
	OpReduceSumF  Opcode = 0x51 // F[dst] = sum(V[src1]) (float)
	OpReduceCount Opcode = 0x52 // R[dst] = count(V[src1])
	OpReduceMin   Opcode = 0x53 // R[dst] = min(V[src1])
	OpReduceMax   Opcode = 0x54 // R[dst] = max(V[src1])
	OpReduceMinF  Opcode = 0x55 // F[dst] = min(V[src1])
	OpReduceMaxF  Opcode = 0x56 // F[dst] = max(V[src1])
	OpReduceMean  Opcode = 0x57 // F[dst] = mean(V[src1])

	// ===== Scalar Operations (0x60-0x6F) =====
	OpMoveR Opcode = 0x60 // R[dst] = R[src1]
	OpMoveF Opcode = 0x61 // F[dst] = F[src1]
	OpAddR  Opcode = 0x62 // R[dst] = R[src1] + R[src2]
	OpSubR  Opcode = 0x63 // R[dst] = R[src1] - R[src2]
	OpMulR  Opcode = 0x64 // R[dst] = R[src1] * R[src2]
	OpDivR  Opcode = 0x65 // R[dst] = R[src1] / R[src2]

	// ===== Frame Operations (0x70-0x7F) =====
	OpNewFrame Opcode = 0x70 // R[dst] = new empty frame
	OpAddCol   Opcode = 0x71 // add V[src1] to frame R[dst] with name constants[imm16]
	OpColCount Opcode = 0x72 // R[dst] = number of columns in frame R[src1]
	OpRowCount Opcode = 0x73 // R[dst] = number of rows in frame R[src1]

	// ===== GroupBy Operations (0x80-0x8F) =====
	OpGroupBy    Opcode = 0x80 // R[dst] = groupby(R[src1] frame, V[src2] key column) -> returns group indices
	OpGroupCount Opcode = 0x81 // V[dst] = count per group from R[src1] groupby result
	OpGroupSum   Opcode = 0x82 // V[dst] = sum(V[src1]) per group from R[src2] groupby result
	OpGroupSumF  Opcode = 0x83 // V[dst] = sum(V[src1]) per group (float)
	OpGroupMin   Opcode = 0x84 // V[dst] = min(V[src1]) per group
	OpGroupMax   Opcode = 0x85 // V[dst] = max(V[src1]) per group
	OpGroupMinF  Opcode = 0x86 // V[dst] = min(V[src1]) per group (float)
	OpGroupMaxF  Opcode = 0x87 // V[dst] = max(V[src1]) per group (float)
	OpGroupMean  Opcode = 0x88 // V[dst] = mean(V[src1]) per group
	OpGroupKeys  Opcode = 0x89 // V[dst] = unique keys from R[src1] groupby result

	// ===== Join Operations (0x90-0x9F) =====
	OpJoinInner Opcode = 0x90 // R[dst] = inner_join(R[src1], R[src2]) on columns specified by imm16
	OpJoinLeft  Opcode = 0x91 // R[dst] = left_join(R[src1], R[src2])
	OpJoinRight Opcode = 0x92 // R[dst] = right_join(R[src1], R[src2])
	OpJoinOuter Opcode = 0x93 // R[dst] = outer_join(R[src1], R[src2])

	// ===== String Operations (0xA0-0xAF) =====
	OpStrLen        Opcode = 0xA0 // V[dst] = strlen(V[src1]) -> int64 column
	OpStrUpper      Opcode = 0xA1 // V[dst] = upper(V[src1])
	OpStrLower      Opcode = 0xA2 // V[dst] = lower(V[src1])
	OpStrConcat     Opcode = 0xA3 // V[dst] = concat(V[src1], V[src2])
	OpStrContains   Opcode = 0xA4 // V[dst] = contains(V[src1], constants[imm16]) -> bool column
	OpStrStartsWith Opcode = 0xA5 // V[dst] = starts_with(V[src1], constants[imm16]) -> bool
	OpStrEndsWith   Opcode = 0xA6 // V[dst] = ends_with(V[src1], constants[imm16]) -> bool
	OpStrTrim       Opcode = 0xA7 // V[dst] = trim(V[src1])
	OpStrSplit      Opcode = 0xA8 // V[dst] = split(V[src1], constants[imm16]) -> first part
	OpStrReplace    Opcode = 0xA9 // V[dst] = replace(V[src1], old, new) using constants

	// ===== Control Flow (0xF0-0xFF) =====
	OpNop   Opcode = 0xF0 // No operation
	OpHaltV Opcode = 0xFD // Stop execution, V[dst] is return value (vector/column)
	OpHalt  Opcode = 0xFE // Stop execution, R[dst] is return value (int64)
	OpHaltF Opcode = 0xFF // Stop execution, F[dst] is return value (float64)
)

// String returns the string representation of an opcode.
func (o Opcode) String() string {
	switch o {
	// Data Loading
	case OpLoadCSV:
		return "LOAD_CSV"
	case OpLoadConst:
		return "LOAD_CONST"
	case OpLoadConstF:
		return "LOAD_CONST_F"
	case OpSelectCol:
		return "SELECT_COL"
	case OpBroadcast:
		return "BROADCAST"
	case OpBroadcastF:
		return "BROADCAST_F"
	case OpLoadFrame:
		return "LOAD_FRAME"
	case OpLoadJSON:
		return "LOAD_JSON"
	case OpLoadParquet:
		return "LOAD_PARQUET"

	// Vector Arithmetic
	case OpVecAddI:
		return "VEC_ADD_I"
	case OpVecSubI:
		return "VEC_SUB_I"
	case OpVecMulI:
		return "VEC_MUL_I"
	case OpVecDivI:
		return "VEC_DIV_I"
	case OpVecModI:
		return "VEC_MOD_I"
	case OpVecAddF:
		return "VEC_ADD_F"
	case OpVecSubF:
		return "VEC_SUB_F"
	case OpVecMulF:
		return "VEC_MUL_F"
	case OpVecDivF:
		return "VEC_DIV_F"

	// Comparison
	case OpCmpEQ:
		return "CMP_EQ"
	case OpCmpNE:
		return "CMP_NE"
	case OpCmpLT:
		return "CMP_LT"
	case OpCmpLE:
		return "CMP_LE"
	case OpCmpGT:
		return "CMP_GT"
	case OpCmpGE:
		return "CMP_GE"

	// Logical
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpNot:
		return "NOT"

	// Filtering
	case OpFilter:
		return "FILTER"
	case OpTake:
		return "TAKE"

	// Aggregations
	case OpReduceSum:
		return "REDUCE_SUM"
	case OpReduceSumF:
		return "REDUCE_SUM_F"
	case OpReduceCount:
		return "REDUCE_COUNT"
	case OpReduceMin:
		return "REDUCE_MIN"
	case OpReduceMax:
		return "REDUCE_MAX"
	case OpReduceMinF:
		return "REDUCE_MIN_F"
	case OpReduceMaxF:
		return "REDUCE_MAX_F"
	case OpReduceMean:
		return "REDUCE_MEAN"

	// Scalar Operations
	case OpMoveR:
		return "MOVE_R"
	case OpMoveF:
		return "MOVE_F"
	case OpAddR:
		return "ADD_R"
	case OpSubR:
		return "SUB_R"
	case OpMulR:
		return "MUL_R"
	case OpDivR:
		return "DIV_R"

	// Frame Operations
	case OpNewFrame:
		return "NEW_FRAME"
	case OpAddCol:
		return "ADD_COL"
	case OpColCount:
		return "COL_COUNT"
	case OpRowCount:
		return "ROW_COUNT"

	// GroupBy Operations
	case OpGroupBy:
		return "GROUP_BY"
	case OpGroupCount:
		return "GROUP_COUNT"
	case OpGroupSum:
		return "GROUP_SUM"
	case OpGroupSumF:
		return "GROUP_SUM_F"
	case OpGroupMin:
		return "GROUP_MIN"
	case OpGroupMax:
		return "GROUP_MAX"
	case OpGroupMinF:
		return "GROUP_MIN_F"
	case OpGroupMaxF:
		return "GROUP_MAX_F"
	case OpGroupMean:
		return "GROUP_MEAN"
	case OpGroupKeys:
		return "GROUP_KEYS"

	// Join Operations
	case OpJoinInner:
		return "JOIN_INNER"
	case OpJoinLeft:
		return "JOIN_LEFT"
	case OpJoinRight:
		return "JOIN_RIGHT"
	case OpJoinOuter:
		return "JOIN_OUTER"

	// String Operations
	case OpStrLen:
		return "STR_LEN"
	case OpStrUpper:
		return "STR_UPPER"
	case OpStrLower:
		return "STR_LOWER"
	case OpStrConcat:
		return "STR_CONCAT"
	case OpStrContains:
		return "STR_CONTAINS"
	case OpStrStartsWith:
		return "STR_STARTS_WITH"
	case OpStrEndsWith:
		return "STR_ENDS_WITH"
	case OpStrTrim:
		return "STR_TRIM"
	case OpStrSplit:
		return "STR_SPLIT"
	case OpStrReplace:
		return "STR_REPLACE"

	// Control Flow
	case OpNop:
		return "NOP"
	case OpHaltV:
		return "HALT_V"
	case OpHalt:
		return "HALT"
	case OpHaltF:
		return "HALT_F"

	default:
		return "UNKNOWN"
	}
}

// OpcodeFromString returns the opcode for the given string.
func OpcodeFromString(s string) (Opcode, bool) {
	switch s {
	// Data Loading
	case "LOAD_CSV":
		return OpLoadCSV, true
	case "LOAD_CONST":
		return OpLoadConst, true
	case "LOAD_CONST_F":
		return OpLoadConstF, true
	case "SELECT_COL":
		return OpSelectCol, true
	case "BROADCAST":
		return OpBroadcast, true
	case "BROADCAST_F":
		return OpBroadcastF, true
	case "LOAD_FRAME":
		return OpLoadFrame, true
	case "LOAD_JSON":
		return OpLoadJSON, true
	case "LOAD_PARQUET":
		return OpLoadParquet, true

	// Vector Arithmetic
	case "VEC_ADD_I":
		return OpVecAddI, true
	case "VEC_SUB_I":
		return OpVecSubI, true
	case "VEC_MUL_I":
		return OpVecMulI, true
	case "VEC_DIV_I":
		return OpVecDivI, true
	case "VEC_MOD_I":
		return OpVecModI, true
	case "VEC_ADD_F":
		return OpVecAddF, true
	case "VEC_SUB_F":
		return OpVecSubF, true
	case "VEC_MUL_F":
		return OpVecMulF, true
	case "VEC_DIV_F":
		return OpVecDivF, true

	// Comparison
	case "CMP_EQ":
		return OpCmpEQ, true
	case "CMP_NE":
		return OpCmpNE, true
	case "CMP_LT":
		return OpCmpLT, true
	case "CMP_LE":
		return OpCmpLE, true
	case "CMP_GT":
		return OpCmpGT, true
	case "CMP_GE":
		return OpCmpGE, true

	// Logical
	case "AND":
		return OpAnd, true
	case "OR":
		return OpOr, true
	case "NOT":
		return OpNot, true

	// Filtering
	case "FILTER":
		return OpFilter, true
	case "TAKE":
		return OpTake, true

	// Aggregations
	case "REDUCE_SUM":
		return OpReduceSum, true
	case "REDUCE_SUM_F":
		return OpReduceSumF, true
	case "REDUCE_COUNT":
		return OpReduceCount, true
	case "REDUCE_MIN":
		return OpReduceMin, true
	case "REDUCE_MAX":
		return OpReduceMax, true
	case "REDUCE_MIN_F":
		return OpReduceMinF, true
	case "REDUCE_MAX_F":
		return OpReduceMaxF, true
	case "REDUCE_MEAN":
		return OpReduceMean, true

	// Scalar Operations
	case "MOVE_R":
		return OpMoveR, true
	case "MOVE_F":
		return OpMoveF, true
	case "ADD_R":
		return OpAddR, true
	case "SUB_R":
		return OpSubR, true
	case "MUL_R":
		return OpMulR, true
	case "DIV_R":
		return OpDivR, true

	// Frame Operations
	case "NEW_FRAME":
		return OpNewFrame, true
	case "ADD_COL":
		return OpAddCol, true
	case "COL_COUNT":
		return OpColCount, true
	case "ROW_COUNT":
		return OpRowCount, true

	// GroupBy Operations
	case "GROUP_BY":
		return OpGroupBy, true
	case "GROUP_COUNT":
		return OpGroupCount, true
	case "GROUP_SUM":
		return OpGroupSum, true
	case "GROUP_SUM_F":
		return OpGroupSumF, true
	case "GROUP_MIN":
		return OpGroupMin, true
	case "GROUP_MAX":
		return OpGroupMax, true
	case "GROUP_MIN_F":
		return OpGroupMinF, true
	case "GROUP_MAX_F":
		return OpGroupMaxF, true
	case "GROUP_MEAN":
		return OpGroupMean, true
	case "GROUP_KEYS":
		return OpGroupKeys, true

	// Join Operations
	case "JOIN_INNER":
		return OpJoinInner, true
	case "JOIN_LEFT":
		return OpJoinLeft, true
	case "JOIN_RIGHT":
		return OpJoinRight, true
	case "JOIN_OUTER":
		return OpJoinOuter, true

	// String Operations
	case "STR_LEN":
		return OpStrLen, true
	case "STR_UPPER":
		return OpStrUpper, true
	case "STR_LOWER":
		return OpStrLower, true
	case "STR_CONCAT":
		return OpStrConcat, true
	case "STR_CONTAINS":
		return OpStrContains, true
	case "STR_STARTS_WITH":
		return OpStrStartsWith, true
	case "STR_ENDS_WITH":
		return OpStrEndsWith, true
	case "STR_TRIM":
		return OpStrTrim, true
	case "STR_SPLIT":
		return OpStrSplit, true
	case "STR_REPLACE":
		return OpStrReplace, true

	// Control Flow
	case "NOP":
		return OpNop, true
	case "HALT_V":
		return OpHaltV, true
	case "HALT":
		return OpHalt, true
	case "HALT_F":
		return OpHaltF, true

	default:
		return 0, false
	}
}
