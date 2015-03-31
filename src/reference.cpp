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
#include <unordered_map>

#include "btree/btree_map.h"
#include "stx/btree_map.h"

#include "Validation.h"

#ifdef CPU
#include <sched.h>
#endif

//#include "ThreadPool.h"
#include <mmintrin.h>	   // MMX
#include <xmmintrin.h>	  // SSE1
#include <emmintrin.h>	  // SSE2

#if (MATH_LEVEL & MATH_LEVEL_SSE3)
#include <pmmintrin.h>	  // Intel SSE3
#endif
#if (MATH_LEVEL & MATH_LEVEL_SSSE3)
#include <tmmintrin.h>	  // Intel SSSE3 (the extra S is not a typo)
#endif
#if (MATH_LEVEL & MATH_LEVEL_SSE4_1)
#include <smmintrin.h>	  // Intel SSE4.1
#endif
#if (MATH_LEVEL & MATH_LEVEL_SSE4_2)
#include <nmmintrin.h>	  // Intel SSE4.2
#endif
#if (MATH_LEVEL & MATH_LEVEL_AES)
#include <wmmintrin.h>	  // Intel AES instructions
#endif
#if (MATH_LEVEL & (MATH_LEVEL_AVX_128|MATH_LEVEL_AVX_256))
#include <immintrin.h>	  // Intel AVX instructions
#endif
//--------------------------------------------------
using namespace std;

//--------------------------------------------------
// Begin reference implementation
//--------------------------------------------------

// create thread pool with 4 worker threads
//ThreadPool pool(THREADS);
static uint32_t relationCount;
static uint32_t* schema;

//unordered_map best performance
static unordered_map<uint32_t, uint64_t*> *relations;

//GLOBAL visible to Validation.h
std::array<uint64_t, MINMAX>***transactionStats;
//--------------------------------------------------
static stx::btree_map<uint64_t, vector<uint64_t*>*> transactionHistory;
static btree::btree_map<uint64_t, bool> queryResults;

#ifdef HISTOGRAM
static unordered_map<uint64_t, uint64_t> histogram;
#endif
//--------------------------------------------------

static void processDefineSchema(const DefineSchema& d) {
#ifdef DEBUG
	cerr << "define schema [" << d.relationCount << "] ";
	for (uint32_t c=0;c!=d.relationCount;++c)
	cerr << d.columnCounts[c] << " ";
	cerr << endl;
#endif
	schema = new uint32_t[d.relationCount];
	memcpy(schema, d.columnCounts, sizeof(uint32_t) * d.relationCount);
	relationCount = d.relationCount;
	relations = new unordered_map<uint32_t, uint64_t*> [d.relationCount];
	transactionStats = new std::array<uint64_t, MINMAX>**[d.relationCount];
	bzero(transactionStats,
			(d.relationCount * sizeof(std::array<uint64_t, MINMAX>*)));
}
//--------------------------------------------------
static void processTransaction(const Transaction& t) {

#ifdef DEBUG
	const char* reader1 = t.operations;
	uint32_t k = 0;
	cerr << "transaction [" << t.transactionId << " " << t.deleteCount << " "
	<< t.insertCount << "] [";

	for (auto dindex = 0; dindex != t.deleteCount; ++dindex) {
		auto o = reinterpret_cast<const TransactionOperationDelete*>(reader1);
		cerr << "[" << o->relationId << "]";
		for (const uint64_t* key = o->keys, *keyLimit = key + o->rowCount;
				key != keyLimit; ++key) {
			auto rel = relations[o->relationId].find(*key);
			if (rel != relations[o->relationId].end()) {
				for (auto k = 0; k != schema[o->relationId]; ++k) {
					cerr << " " << k<< "(" << rel->second[k] << ")";

				}

			}
		}
		reader1 += sizeof(TransactionOperationDelete)
		+ (sizeof(uint64_t) * o->rowCount);

	}
	cerr << "] ";
	k = 0;
	for (auto dindex = 0; dindex != t.insertCount; ++dindex) {
		auto ins = reinterpret_cast<const TransactionOperationInsert*>(reader1);
		cerr << "[" << ins->relationId << "]";
		for (const uint64_t* values = ins->values, *valuesLimit = values
				+ (ins->rowCount * schema[ins->relationId]);
				values != valuesLimit; values += schema[ins->relationId]) {
			for (auto k = 0; k != schema[ins->relationId]; ++k) {
				cerr << " " << k<< "(" << values[k] << ")";

			}
		}

		reader1 += sizeof(TransactionOperationInsert)
		+ (sizeof(uint64_t) * ins->rowCount * schema[ins->relationId]);

	}
	cerr << "]" << endl;
#endif
	vector<uint64_t*>* operations = new vector<uint64_t*> [relationCount];
	uint32_t index;
	const TransactionOperationDelete* o;
	const TransactionOperationInsert* ins;
	unordered_map<uint32_t, uint64_t*>::iterator rel;
	uint64_t* tuple;
	uint32_t i;
	const char* reader = t.operations;

	// Delete all indicated tuples
	for (index = 0; index != t.deleteCount; ++index) {
		o = reinterpret_cast<const TransactionOperationDelete*>(reader);
		for (const uint64_t* key = o->keys, *keyLimit = key + o->rowCount;
				key != keyLimit; ++key) {
			rel = relations[o->relationId].find(*key);
			if (rel != relations[o->relationId].end()) {
				operations[o->relationId].emplace_back(rel->second);
				relations[o->relationId].erase(rel);
			}
		}
		reader += sizeof(TransactionOperationDelete)
				+ (sizeof(uint64_t) * o->rowCount);
	}

	// Insert new tuples
	for (index = 0; index != t.insertCount; ++index) {
		ins = reinterpret_cast<const TransactionOperationInsert*>(reader);
		for (const uint64_t* values = ins->values, *valuesLimit = values
				+ (ins->rowCount * schema[ins->relationId]);
				values != valuesLimit; values += schema[ins->relationId]) {
			tuple = new uint64_t[schema[ins->relationId]];
			memcpy(tuple, values, schema[ins->relationId] * sizeof(uint64_t));
			operations[ins->relationId].emplace_back(tuple);
			relations[ins->relationId][values[0]] = tuple;

			if (transactionStats[ins->relationId] == NULL) {
				transactionStats[ins->relationId] = new std::array<uint64_t,
				MINMAX>*[schema[ins->relationId]];
				bzero(transactionStats[ins->relationId],
						(schema[ins->relationId] * sizeof(std::array<uint64_t,
						MINMAX>*)));
			}

			//save stats
			for (i = 0; i != schema[ins->relationId]; ++i) {
#ifdef HISTOGRAM
				if (histogram.find(values[i]) == histogram.end()) {
					histogram[values[i]] = 1;
				} else {
					histogram[values[i]] = histogram[values[i]] + 1;
				}
#endif
				if (transactionStats[ins->relationId][i] == NULL) {
					transactionStats[ins->relationId][i] = new std::array<
							uint64_t,
							MINMAX>;
					auto& minmax = *transactionStats[ins->relationId][i];
					minmax[MIN] = values[i];
					minmax[MAX] = values[i];
				} else {
					auto& minmax = *transactionStats[ins->relationId][i];
					if (minmax[MIN] > values[i]) {
						minmax[MIN] = values[i];
					} else if (minmax[MAX] < values[i]) {
						minmax[MAX] = values[i];
					}
				}
			}
		}

		reader += sizeof(TransactionOperationInsert)
				+ (sizeof(uint64_t) * ins->rowCount * schema[ins->relationId]);
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
	Query* queries[v.queryCount];
	uint32_t pos = 0;
	uint32_t index;
	Query *q;
	stx::btree_map<uint64_t, vector<uint64_t*>*>::iterator iter;
	bool match;
	Query::Column * c;
	Query::Column * cLimit;
	uint64_t tupleValue;
	uint64_t queryValue;
	bool result;

	for (index = 0; index != v.queryCount; ++index) {
		q = const_cast<Query*>(reinterpret_cast<const Query*>(reader));
//		++all;

		if (isQueryValid(q)) {
			if (q->columnCount == 0) {
				for (iter = from; iter != to; ++iter) {
					if (iter->second[q->relationId].size()) {
						queryResults[v.validationId] = true;
						return;
					}
				}
			}
			queries[pos] = q;
			++pos;
		}
//		else {
//			++pruned;
//		}
		reader += sizeof(Query) + (sizeof(Query::Column) * q->columnCount);
	}
//	if (pos <= 0) {
//		queryResults[v.validationId] = conflict;
//		return;
//	}
///////////////////////////////////////////////////////////////////////

	for (iter = from; iter != to; ++iter) {
//Iterates through all sorted queries
		for (index = 0; index != pos; ++index) {
			q = queries[index];

			//std::sort(q->columns, q->columns + q->columnCount, columnComparator);
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

			for (auto& tuple : iter->second[q->relationId]) {

				match = true;
				c = q->columns;

				cLimit = c + q->columnCount;
				for (; c != cLimit; ++c) {
					tupleValue = tuple[c->column];
					queryValue = c->value;
					result = false;
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
						continue;
//						queryResults[v.validationId] = true;
//						return;
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
//	auto start = queryResults.begin();
//	auto stop  = queryResults.find(f.validationId);
//
//	for(auto s=start; s!=stop; ++s) {
//		char c = '0' + s->second;
//		cout.write(&c, 1);
//		queryResults.erase(s);
//	}
//	cout.flush();
	char c;
	while ((!queryResults.empty())
			&& ((*queryResults.begin()).first <= f.validationId)) {
		c = '0' + (*queryResults.begin()).second;
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
//transactionHistory.erase(transactionHistory.begin(), transactionHistory.find(f.transactionId));
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
#ifdef CPU
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(2, &mask);
	sched_setaffinity(0, sizeof(mask), &mask);
#endif
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
#ifdef HISTOGRAM
			for (auto iter : histogram) {
				cerr << iter.first << "|" << iter.second << endl;
			}
#endif

//			cerr << "All=" << all << " Pruned=" << pruned << " Percentage="
//					<< pruned / all << endl;
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
