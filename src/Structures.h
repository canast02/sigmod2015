/*
 * structures.h
 *
 *  Created on: Mar 2, 2015
 *      Author: costantinos
 */

#ifndef STRUCTURES_H_
#define STRUCTURES_H_

#define OPERATORS 7
//#define THREADS 4
//#define DEBUG

//--------------------------------------------------
// Wire protocol messages
//--------------------------------------------------
struct MessageHead {
	/// Message types
	enum Type
		: uint32_t {Done,
		DefineSchema,
		Transaction,
		ValidationQueries,
		Flush,
		Forget
	};
	/// Total message length, excluding this head
	uint32_t messageLen;
	/// The message type
	Type type;
};
//--------------------------------------------------
struct DefineSchema {
	/// Number of relations
	uint32_t relationCount;
	/// Column counts per relation, one count per relation. The first column is always the primary key
	uint32_t columnCounts[];
};
//--------------------------------------------------
struct Transaction {
	/// The transaction id. Monotonic increasing
	uint64_t transactionId;
	/// The operation counts
	uint32_t deleteCount, insertCount;
	/// A sequence of transaction operations. Deletes first, total deleteCount+insertCount operations
	char operations[];
};
//--------------------------------------------------
struct TransactionOperationDelete {
	/// The affected relation
	uint32_t relationId;
	/// The row count
	uint32_t rowCount;
	/// The deleted values, rowCount primary keyss
	uint64_t keys[];
};
//--------------------------------------------------
struct TransactionOperationInsert {
	/// The affected relation
	uint32_t relationId;
	/// The row count
	uint32_t rowCount;
	/// The inserted values, rowCount*relation[relationId].columnCount values
	uint64_t values[];
};
//--------------------------------------------------
struct ValidationQueries {
	/// The validation id. Monotonic increasing
	uint64_t validationId;
	/// The transaction range
	uint64_t from, to;
	/// The query count
	uint32_t queryCount;
	/// The queries
	char queries[];
};
//--------------------------------------------------
struct Query {
	/// A column description
	struct Column {
		/// Support operations
		enum Op
			: uint32_t {Equal,
			NotEqual,
			Less,
			LessOrEqual,
			Greater,
			GreaterOrEqual,
			Invalid
		};
		/// The column id
		uint32_t column;
		/// The operations
		Op op;
		/// The constant
		uint64_t value;
	};
	/// The relation
	uint32_t relationId;
	/// The number of bound columns
	uint32_t columnCount;
	/// The bindings
	Column columns[];
};

//--------------------------------------------------
struct OPtuple {
	uint64_t** tuples = NULL;
	int count;
	OPtuple(void) :
			count(-1) {
	}
};

inline static bool queryComparator(const Query* q1, const Query* q2) {
	return (q1->columnCount < q2->columnCount);
}
inline static bool columnComparator(Query::Column& c1, Query::Column& c2) {
	return (c1.op < c2.op);
}

inline static bool columnOpComparator(Query::Column& c1, Query::Column& c2) {
	return (c1.column < c2.column);
}

//--------------------------------------------------
struct Flush {
	/// All validations to this id (including) must be answered
	uint64_t validationId;
};
//--------------------------------------------------
struct Forget {
	/// Transactions older than that (including) will not be tested for
	uint64_t transactionId;
};
#ifdef DEBUG
int Equal = 0, NotEqual = 0, Less = 0, LessOrEqual = 0, Greater = 0,
GreaterOrEqual = 0, Invalid = 0;
#endif

#endif /* STRUCTURES_H_ */
