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
unordered_map<uint32_t, uint64_t[MINMAX]> *globalStats;
unordered_map<uint64_t,
		unordered_map<uint32_t, unordered_map<uint32_t, uint64_t[MINMAX]>>> transactionStats;

//--------------------------------------------------
static stx::btree_map<uint64_t, vector<uint64_t*>*> transactionHistory;
static btree::btree_map<uint64_t, bool> queryResults;
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
	globalStats =
			new unordered_map<uint32_t, uint64_t[MINMAX]> [d.relationCount];

}
//--------------------------------------------------
static void processTransaction(const Transaction& t) {
#ifdef DEBUG
	cerr << "transaction [" << t.transactionId << " " << t.deleteCount << " "
	<< t.insertCount << "]" << endl;
#endif
	vector<uint64_t*>* operations = new vector<uint64_t*> [relationCount];
	const char* reader = t.operations;

	//create per transaction minmax
	auto& tLocalMap = transactionStats[t.transactionId];

	// Delete all indicated tuples
	for (uint32_t index = 0; index != t.deleteCount; ++index) {
		auto& o = *reinterpret_cast<const TransactionOperationDelete*>(reader);
		for (const uint64_t* key = o.keys, *keyLimit = key + o.rowCount;
				key != keyLimit; ++key) {
			auto rel = relations[o.relationId].find(*key);
			if (rel != relations[o.relationId].end()) {
				/*****************************************************/
				//save stats
				/*****************************************************/
				uint64_t* values = rel->second;
				auto tStatsIterEnd = globalStats[o.relationId].end();
				auto tLocalStatsIterEnd = tLocalMap[o.relationId].end();
				for (uint32_t i = 0; i != schema[o.relationId]; ++i) {
					/////////////////////////////////////////////
					//Local stats
					/////////////////////////////////////////////
					auto tLocalStatsIter = tLocalMap[o.relationId].find(i);
					if (tLocalStatsIter == tLocalStatsIterEnd) {
						//create new 'i'
						auto& minmax = tLocalMap[o.relationId][i];
						minmax[MIN] = values[i];
						minmax[MAX] = values[i];
					} else {
						auto& minmax = tLocalStatsIter->second;
						if (minmax[MIN] > values[i]) {
							minmax[MIN] = values[i];
						} else if (minmax[MAX] < values[i]) {
							minmax[MAX] = values[i];
						}
					}

					/////////////////////////////////////////////
					//Global stats
					/////////////////////////////////////////////
					auto tStatsIter = globalStats[o.relationId].find(i);
					if (tStatsIter == tStatsIterEnd) {
						//create new 'i'
						auto& minmax = globalStats[o.relationId][i];
						minmax[MIN] = values[i];
						minmax[MAX] = values[i];
					} else {

						auto& minmax = tStatsIter->second;
						if (minmax[MIN] > values[i]) {
							minmax[MIN] = values[i];
						} else if (minmax[MAX] < values[i]) {
							minmax[MAX] = values[i];
						}
					}

				}

				/*****************************************************/
				//save stats
				/*****************************************************/

				operations[o.relationId].emplace_back(move(rel->second));
				relations[o.relationId].erase(rel);
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
			operations[o.relationId].emplace_back(tuple);
			relations[o.relationId][values[0]] = tuple;

			/*****************************************************/
			//save stats
			/*****************************************************/
			auto tStatsIterEnd = globalStats[o.relationId].end();
			auto tLocalStatsIterEnd = tLocalMap[o.relationId].end();
			for (uint32_t i = 0; i != schema[o.relationId]; ++i) {
				/////////////////////////////////////////////
				//Local stats
				/////////////////////////////////////////////
				auto tLocalStatsIter = tLocalMap[o.relationId].find(i);
				if (tLocalStatsIter == tLocalStatsIterEnd) {
					//create new 'i'
					auto& minmax = tLocalMap[o.relationId][i];

					minmax[MIN] = values[i];
					minmax[MAX] = values[i];
				} else {

					auto& minmax = tLocalStatsIter->second;
					if (minmax[MIN] > values[i]) {
						minmax[MIN] = values[i];
					} else if (minmax[MAX] < values[i]) {
						minmax[MAX] = values[i];
					}
				}
				/////////////////////////////////////////////
				//Global stats
				/////////////////////////////////////////////
				auto tStatsIter = globalStats[o.relationId].find(i);
				if (tStatsIter == tStatsIterEnd) {
					//create new 'i'
					auto& minmax = globalStats[o.relationId][i];
					minmax[MIN] = values[i];
					minmax[MAX] = values[i];
				} else {

					auto& minmax = tStatsIter->second;
					if (minmax[MIN] > values[i]) {
						minmax[MIN] = values[i];
					} else if (minmax[MAX] < values[i]) {
						minmax[MAX] = values[i];
					}
				}
			}
			/*****************************************************/
			//save stats
			/*****************************************************/

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
	uint32_t pos = 0;

	for (unsigned index = 0; index != v.queryCount; ++index) {
		auto q = const_cast<Query*>(reinterpret_cast<const Query*>(reader));

		if (isGlobalQueryValid(q)) {
			queries[pos] = q;
			pos++;
		} else if (isQueryValid(q)) {
			queries[pos] = q;
			pos++;
		}
		reader += sizeof(Query) + (sizeof(Query::Column) * q->columnCount);
	}

///////////////////////////////////////////////////////////////////////
	//std::sort(queries, queries + pos, queryComparator);
//Iterates through all sorted queries
	for (unsigned index = 0; index != pos; ++index) {
		auto q = queries[index];
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
		for (auto iter = from; iter != to; ++iter) {
			/*			if ((*iter).second[q->relationId].size() == 0)
			 continue;
			 if ((*iter).second[q->relationId].size() >= 10) {
			 auto c = q->columns;
			 bool match = true;
			 auto cLimit = c + q->columnCount;
			 auto tStatsIterEnd = globalStats[q->relationId].end();
			 for (; c != cLimit; ++c) {
			 bool result = true;
			 auto tStatsIter = globalStats[q->relationId].find(
			 c->column);

			 if (tStatsIter != tStatsIterEnd) {
			 auto& minmax = tStatsIter->second;
			 switch (c->op) {

			 case Query::Column::Equal:
			 if (c->value > minmax[MAX]
			 || c->value < minmax[MIN]) {
			 result = false;
			 }
			 break;
			 case Query::Column::NotEqual:
			 if (c->value == minmax[MAX]
			 && minmax[MAX] == minmax[MIN]) {

			 result = false;
			 }
			 break;
			 case Query::Column::Less:
			 if (c->value <= minmax[MIN]) {
			 result = false;
			 }
			 break;
			 case Query::Column::LessOrEqual:

			 if (c->value < minmax[MIN])
			 result = false;
			 break;
			 case Query::Column::Greater:
			 if (c->value >= minmax[MAX])
			 result = false;
			 break;
			 case Query::Column::GreaterOrEqual:
			 if (c->value > minmax[MAX])
			 result = false;
			 break;
			 case Query::Column::Invalid:
			 break;
			 }
			 }
			 if (!result) {
			 match = false;
			 break;
			 }
			 }
			 if (!match)
			 continue;
			 }
			 */
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
//transactionHistory.erase(transactionHistory.begin(), transactionHistory.find(f.transactionId));
	while ((!transactionHistory.empty())
			&& ((*transactionHistory.begin()).first <= f.transactionId)) {
		//transactionStats.erase(transactionHistory.begin()->first);
		transactionHistory.erase(transactionHistory.begin());
	}
	/*
	 delete[] globalStats;
	 globalStats = new unordered_map<uint32_t, uint64_t[MINMAX]> [relationCount];
	 //For each transaction in the transaction stats
	 for (auto& cur : transactionStats) {
	 if (cur.first <= f.transactionId)
	 continue;
	 //For each relation per transaction
	 for (auto& relIter : cur.second) {
	 //Update Global stats
	 auto tStatsIterEnd = globalStats[relIter.first].end();
	 for (auto& colIter : relIter.second) {
	 auto tStatsIter = globalStats[relIter.first].find(
	 colIter.first);
	 if (tStatsIter == tStatsIterEnd) {
	 //create new 'i'
	 auto& minmax = globalStats[relIter.first][colIter.first];
	 minmax[MIN] = colIter.second[MIN];
	 minmax[MAX] = colIter.second[MAX];
	 } else {

	 auto& minmax = tStatsIter->second;

	 if (minmax[MIN] > colIter.second[MIN]) {
	 minmax[MIN] = colIter.second[MIN];
	 }
	 if (minmax[MAX] < colIter.second[MAX]) {
	 minmax[MAX] = colIter.second[MAX];
	 }
	 }

	 }
	 }
	 }
	 */
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
