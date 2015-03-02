// SIGMOD Programming Contest 2015 - reference implementation
//
// This code is intended to illustrate the given challenge, it
// is not particular efficient.
//--------------------------------------------------
// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <http://unlicense.org/>
//--------------------------------------------------
#include <ios>
#include <iostream>
#include <cassert>
#include <cstdint>
#include "btree/btree_map.h"
#include "Validation.h"
//#include "ThreadPool.h"

//--------------------------------------------------
using namespace std;

//--------------------------------------------------
// Begin reference implementation
//--------------------------------------------------

// create thread pool with 4 worker threads
//ThreadPool pool(THREADS);
static uint32_t relationCount;
static uint32_t* schema;
static unordered_map<uint32_t, uint64_t*> *relations;
//--------------------------------------------------
static btree::btree_map<uint64_t, vector<uint64_t*>*> transactionHistory;
static btree::btree_map<uint64_t, bool> queryResults;
//--------------------------------------------------

static void processDefineSchema(const DefineSchema& d) {
#ifdef DEBUG
	cerr << "define schema [" << d.relationCount << " ";
	for (auto& c : d.columnCounts)
	cerr << c << " ";
	cerr << endl;
#endif
	schema = new uint32_t[d.relationCount];
	memcpy(schema, d.columnCounts, sizeof(uint32_t) * d.relationCount);
	relationCount = d.relationCount;
	relations = new unordered_map<uint32_t, uint64_t*> [d.relationCount];

}
//--------------------------------------------------
static void processTransaction(const Transaction& t) {
#ifdef DEBUG
	cerr << "transaction [" << t.transactionId << " " << t.deleteCount << " "
	<< t.insertCount << "]" << endl;
#endif
	vector<uint64_t*>* operations = new vector<uint64_t*>[relationCount];
	const char* reader = t.operations;

	// Delete all indicated tuples
	for (uint32_t index = 0; index != t.deleteCount; ++index) {
		auto& o = *reinterpret_cast<const TransactionOperationDelete*>(reader);
		for (const uint64_t* key = o.keys, *keyLimit = key + o.rowCount;
				key != keyLimit; ++key) {
			if (relations[o.relationId].count(*key)) {
//				if (operations.find(o.relationId) == operations.end()) {
//					vector<uint64_t*> v;
//					operations[o.relationId] = move(v);
//				}
				operations[o.relationId].emplace_back(
						move(relations[o.relationId][*key]));
				relations[o.relationId].erase(*key);
			}
		}
		reader += sizeof(TransactionOperationDelete)
				+ (sizeof(uint64_t) * o.rowCount);
	}

	// Insert new tuples
	for (uint32_t index = 0; index != t.insertCount; ++index) {
		auto& o = *reinterpret_cast<const TransactionOperationInsert*>(reader);
		for (const uint64_t* values = o.values, *valuesLimit = values
				+ (o.rowCount * schema[o.relationId]); values != valuesLimit;
				values += schema[o.relationId]) {
			uint64_t* tuple = new uint64_t[schema[o.relationId]];
			memcpy(tuple, values, schema[o.relationId] * sizeof(uint64_t));

			//tuple.insert(tuple.begin(), values, values + schema[o.relationId]);
//			if (operations.find(o.relationId) == operations.end()) {
//				vector<uint64_t*> v;
//				operations[o.relationId] = move(v);
//			}
			operations[o.relationId].emplace_back(tuple);
			relations[o.relationId][values[0]] = tuple;
		}
		reader += sizeof(TransactionOperationInsert)
				+ (sizeof(uint64_t) * o.rowCount * schema[o.relationId]);
	}

	transactionHistory[t.transactionId] = operations;
}
//--------------------------------------------------

static void processValidationQueries(const ValidationQueries& v) {

#ifdef DEBUG
	cerr << "validation [" << v.validationId << " " << v.from << " " << v.to
	<< " " << v.queryCount << "]" << endl;
	unsigned qId = 0;
#endif
	auto from = transactionHistory.lower_bound(v.from);
	auto to = transactionHistory.upper_bound(v.to);
	bool conflict = false;
	const char* reader = v.queries;

	///////////////////////////////////////////////////////////////////////
	//Sort the query based on the validation's query count
	///////////////////////////////////////////////////////////////////////
	Query* queries[v.queryCount];
//	int pruned = 0;
//	int invalid = 0;
	unsigned pos = 0;
	for (unsigned index = 0; index != v.queryCount; ++index) {
		auto q = const_cast<Query*>(reinterpret_cast<const Query*>(reader));
		if (isQueryValid(q)) {
			queries[pos] = q;
			pos++;
		}

//		else {
//			++pruned;
//		}
		reader += sizeof(Query) + (sizeof(Query::Column) * q->columnCount);
	}

	//cerr << pruned << "|" << v.queryCount << endl;
	std::sort(queries, queries + pos, queryComparator);

	///////////////////////////////////////////////////////////////////////

	//Iterates through all sorted queries
	for (unsigned index = 0; index != pos; ++index) {
		auto q = queries[index];
		std::sort(q->columns, q->columns + q->columnCount, columnComparator);

#ifdef DEBUG
		cerr << "q" << qId << "[ ";
		qId++;
		cerr << q->relationId << " ";
		for (unsigned i = 0; i < q->columnCount; i++) {
			cerr << "c" << q->columns[i].column << " ";
			switch (q->columns[i].op) {
				case Query::Column::Equal:
				cerr << "= ";
				break;
				case Query::Column::NotEqual:
				cerr << "!= ";
				break;
				case Query::Column::Less:
				cerr << "< ";
				break;
				case Query::Column::LessOrEqual:
				cerr << "<= ";
				break;
				case Query::Column::Greater:
				cerr << "> ";
				break;
				case Query::Column::GreaterOrEqual:
				cerr << ">= ";
				break;
				case Query::Column::Invalid:
				cerr << ".|. ";
				break;
			}
			cerr << q->columns[i].value << " ";
		}

		cerr << "]"<< endl;
#endif

//		cerr << "Equal =" << Equal << ", NotEqual =" << NotEqual << ", Less ="
//				<< Less << ", LessOrEqual =" << LessOrEqual << ", Greater ="
//				<< Greater << ", GreaterOrEqual =" << GreaterOrEqual << endl;
		for (auto iter = from; iter != to; ++iter) {

			for (auto& tuple : (*iter).second[q->relationId]) {

				bool match = true;
				auto c = q->columns;

				auto cLimit = c + q->columnCount;
				for (; c != cLimit; ++c) {
					uint64_t tupleValue = tuple[c->column];
					uint64_t queryValue = c->value;
					bool result = false;
					switch (c->op) {
					case Query::Column::Equal:
						result = (tupleValue == queryValue);
						break;
					case Query::Column::NotEqual:
						result = (tupleValue != queryValue);
						break;
					case Query::Column::Less:
						result = (tupleValue < queryValue);
						break;
					case Query::Column::LessOrEqual:
						result = (tupleValue <= queryValue);
						break;
					case Query::Column::Greater:
						result = (tupleValue > queryValue);
						break;
					case Query::Column::GreaterOrEqual:
						result = (tupleValue >= queryValue);
						break;
					case Query::Column::Invalid:
//						++invalid;
						queryResults[v.validationId] = true;
						return;
					}
					if (!result) {
						match = false;
						break;
					}
				}
				if (match) {
					conflict = true;
					break;
				}
			}

			if (conflict) {
				break;
			}
		}

		if (conflict) {
			break;
		}
	}
//	if (invalid > 0)
//		cerr << "invalid:" << invalid << endl;
	queryResults[v.validationId] = conflict;
}
//--------------------------------------------------
static void processFlush(const Flush& f) {
#ifdef DEBUG
	cerr << "flush [" << f.validationId << "]" << endl;
#endif
	while ((!queryResults.empty())
			&& ((*queryResults.begin()).first <= f.validationId)) {
		char c = '0' + (*queryResults.begin()).second;
		cout.write(&c, 1);
		queryResults.erase(queryResults.begin());
	}
	cout.flush();
}
//--------------------------------------------------
static void processForget(const Forget& f) {
#ifdef DEBUG
	cerr << "forget [" << f.transactionId << "]" << endl;
#endif
	while ((!transactionHistory.empty())
			&& ((*transactionHistory.begin()).first <= f.transactionId))
		transactionHistory.erase(transactionHistory.begin());
}
//--------------------------------------------------
// Read the message body and cast it to the desired type
template<typename Type> static const Type& readBody(istream& in,
		vector<char>& buffer, uint32_t len) {
	buffer.resize(len);
	in.read(buffer.data(), len);
	return *reinterpret_cast<const Type*>(buffer.data());
}
//--------------------------------------------------
int main() {
	vector<char> message;
	std::ios_base::sync_with_stdio(false);
	while (true) {
// Retrieve the message
		MessageHead head;
		cin.read(reinterpret_cast<char*>(&head), sizeof(head));
		if (!cin) {
			cerr << "read error" << endl;
			abort();
		} // crude error handling, should never happen

// And interpret it
		switch (head.type) {
		case MessageHead::Done:
			return 0;
		case MessageHead::DefineSchema:
			processDefineSchema(
					readBody<DefineSchema>(cin, message, head.messageLen));
			break;
		case MessageHead::Transaction:
			processTransaction(
					readBody<Transaction>(cin, message, head.messageLen));
			break;
		case MessageHead::ValidationQueries:
			processValidationQueries(
					readBody<ValidationQueries>(cin, message, head.messageLen));
			break;
		case MessageHead::Flush:
			processFlush(readBody<Flush>(cin, message, head.messageLen));
			break;
		case MessageHead::Forget:
			processForget(readBody<Forget>(cin, message, head.messageLen));
			break;
		default:
			cerr << "malformed message" << endl;
			abort(); // crude error handling, should never happen
		}
	}
}
//--------------------------------------------------
