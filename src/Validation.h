/*
 * validation.h
 *
 *  Created on: Mar 2, 2015
 *      Author: costantinos
 */

#ifndef VALIDATION_H_
#define VALIDATION_H_

#include <unordered_map>
#include <algorithm>    // std::sort
#include <vector>
#include <array>

#include "Structures.h"
#include "sparsehash/dense_hash_map.h"
using namespace std;

extern vector<std::array<uint64_t, MINMAX>>*globalStats;
extern unordered_map<uint64_t,
		unordered_map<uint32_t, vector<std::array<uint64_t, MINMAX>>> > transactionStats;

inline static unordered_map<uint32_t, vector<std::array<uint64_t, MINMAX>>>* rangedMinMax(
		const uint64_t from, const uint64_t to) {
	unordered_map<uint32_t,vector<std::array<uint64_t,MINMAX>>>* rangedStatP =
	new unordered_map<uint32_t,vector<std::array<uint64_t,MINMAX>>>();

	auto& rangedStats = *rangedStatP;
	//For each transaction in the transaction stats
	for (auto& cur : transactionStats) {
		if (cur.first < from || cur.first > to)
		continue;

		//For each relation per transaction
		for (auto& relIter : cur.second) {
			//Update Global stats
			for (uint32_t i = 0; i != relIter.second.size(); ++i) {
				if (rangedStats[relIter.first].size()<=i) {
					//create new 'i'
					auto& mine=relIter.second[i];
					std::array<uint64_t, MINMAX> minmax;
					minmax[MIN] = mine[MIN];
					minmax[MAX] = mine[MAX];
					rangedStats[relIter.first].emplace_back(move(minmax));
				} else {

					auto& minmax = rangedStats[relIter.first][i];

					if (minmax[MIN] > relIter.second[i][MIN]) {
						minmax[MIN] = relIter.second[i][MIN];
					}
					if (minmax[MAX] < relIter.second[i][MAX]) {
						minmax[MAX] = relIter.second[i][MAX];
					}
				}

			}
		}
	}

	return rangedStatP;

}

inline static bool isQueryValid(Query* q) {
	/**
	 * To value-initialize an object of type T means:
	 * — if T is a (possibly cv-qualified) class type without a user-provided or deleted default constructor,
	 * then the object is zero-initialized …, and if T has a non-trivial default constructor,
	 * the object is default-initialized;
	 */
	stx::btree_map<uint32_t, std::array<Query::Column*, OPERATORS>> opsMap;
	//std::sort(q->columns, q->columns + q->columnCount, columnComparator);
	/**
	 * IMPORTANT:
	 * dense_hash_map requires you call set_empty_key() immediately after constructing the hash-map,
	 * and before calling any other dense_hash_map method.
	 */
	//opsMap.set_empty_key(0);
	//store end iteration
	auto opsMapIterEnd = opsMap.end();
	for (unsigned i = 0; i < q->columnCount; i++) {
		auto curQueryOp = &(q->columns[i]);
		//for each column check
		auto opsMapIter = opsMap.find(curQueryOp->column);
		if (opsMapIter == opsMapIterEnd) {
			opsMap[curQueryOp->column][curQueryOp->op] = curQueryOp;
		} else {
			//Get the column from the columns map
			std::array<Query::Column*, OPERATORS> opColumn = opsMapIter->second;

			//Fill the operation list
			if (opColumn[curQueryOp->op] == NULL
					|| opColumn[curQueryOp->op]->op == Query::Column::Invalid) {
				//save
				opColumn[curQueryOp->op] = curQueryOp;
			}

			//check the operations of a specific column
			switch (curQueryOp->op) {
			case Query::Column::Equal:
#ifdef DEBUG
				++Equal;
#endif
				//Check c0==<value1> && c0==<value2>, value1!=value2
				if (opColumn[Query::Column::Equal] != NULL
						&& opColumn[Query::Column::Equal]->op
								!= Query::Column::Invalid
						&& opColumn[Query::Column::Equal] != curQueryOp) {
					if (opColumn[Query::Column::Equal]->value
							!= curQueryOp->value) {
#ifdef DEBUG
						cerr << "==" << endl;
#endif
						return false;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}
				}
				//Check c0!=<value1> && c0==<value2>, value1==value2
				if (opColumn[Query::Column::NotEqual] != NULL
						&& opColumn[Query::Column::NotEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::NotEqual]->value
							== curQueryOp->value) {
#ifdef DEBUG
						cerr << "!=" << endl;
#endif
						return false;
					} else {
						opColumn[Query::Column::NotEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::NotEqual] = NULL;
					}
				}
				//Check c0<=<value1> && c0==<value2>, value1<value2
				if (opColumn[Query::Column::LessOrEqual] != NULL
						&& opColumn[Query::Column::LessOrEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::LessOrEqual]->value
							< curQueryOp->value) {
#ifdef DEBUG
						cerr << "<=" << endl;
#endif
						return false;
					} else {
						opColumn[Query::Column::LessOrEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::LessOrEqual] = NULL;
					}
				}
				//Check c0>=<value1> && c0==<value2>, value1==value2
				if (opColumn[Query::Column::GreaterOrEqual] != NULL
						&& opColumn[Query::Column::GreaterOrEqual]->op
								!= Query::Column::Invalid) {

					if (opColumn[Query::Column::GreaterOrEqual]->value
							> curQueryOp->value) {
#ifdef DEBUG
						cerr << ">=" << endl;
#endif
						return false;
					} else {
						opColumn[Query::Column::GreaterOrEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::GreaterOrEqual] = NULL;
					}
				}
				//Check c0<<value1> && c0==<value2>, value1<=value2
				if (opColumn[Query::Column::Less] != NULL
						&& opColumn[Query::Column::Less]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Less]->value
							<= curQueryOp->value) {
#ifdef DEBUG
						cerr << "<" << endl;
#endif
						return false;
					} else {
						opColumn[Query::Column::Less]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::Less] = NULL;
					}
				}
				//Check c0><value1> && c0==<value2>, value1>=value2
				if (opColumn[Query::Column::Greater] != NULL
						&& opColumn[Query::Column::Greater]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Greater]->value
							>= curQueryOp->value) {
#ifdef DEBUG
						cerr << ">" << endl;
#endif
						return false;
					} else {
						opColumn[Query::Column::Greater]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::Greater] = NULL;
					}
				}
				break;
////////////////////////////////////////////////////////////////////////////////////////
				//Dont do anything
			case Query::Column::NotEqual:
#ifdef DEBUG
				++NotEqual;
#endif
				break;
////////////////////////////////////////////////////////////////////////////////////////
			case Query::Column::Less:
#ifdef DEBUG
				++Less;
#endif
				//Check c0==value1> && c0<<value2>, value1>=value2
				if (opColumn[Query::Column::Equal] != NULL
						&& opColumn[Query::Column::Equal]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Equal]->value
							>= curQueryOp->value) {
						return false;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}

				}
				//Check c0><value1> && c0<<value2>, value1>=value2
				if (opColumn[Query::Column::Greater] != NULL
						&& opColumn[Query::Column::Greater]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Greater]->value
							>= curQueryOp->value) {
						return false;
					}
				}
				//Check c0>=<value1> && c0<<value2>, value1>=value2
				if (opColumn[Query::Column::GreaterOrEqual] != NULL
						&& opColumn[Query::Column::GreaterOrEqual]->op
								!= Query::Column::Invalid) {

					if (opColumn[Query::Column::GreaterOrEqual]->value
							>= curQueryOp->value) {
						return false;
					}
				}

				//Check c0!=<value1> && c0<<value2>, value1<=value2
				if (opColumn[Query::Column::NotEqual] != NULL
						&& opColumn[Query::Column::NotEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::NotEqual]->value
							> curQueryOp->value) {
						//TODO: remove != from queries
						opColumn[Query::Column::NotEqual]->op =
								Query::Column::Invalid;

					}
				}

				//Check c0<=<value1> && c0<<value2>
				if (opColumn[Query::Column::LessOrEqual] != NULL
						&& opColumn[Query::Column::LessOrEqual]->op
								!= Query::Column::Invalid) {
					//TODO: keep the smallest one from queries
					if (opColumn[Query::Column::LessOrEqual]->value
							>= curQueryOp->value) {
						opColumn[Query::Column::LessOrEqual]->op =
								Query::Column::Invalid;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}
				}

				//Check c0<<value1> && c0<<value2>
				if (opColumn[Query::Column::Less] != NULL
						&& opColumn[Query::Column::Less]->op
								!= Query::Column::Invalid
						&& opColumn[Query::Column::Less] != curQueryOp) {
					//TODO: Keep the smallest
					if (curQueryOp->value
							<= opColumn[Query::Column::Less]->value) {
						opColumn[Query::Column::Less]->op =
								Query::Column::Invalid;
						opColumn[Query::Column::Less] = curQueryOp;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}
				}

				break;
//////////////////////////////////////////////////////////////////////////////////////////
			case Query::Column::LessOrEqual:
#ifdef DEBUG
				++LessOrEqual;
#endif
				//Check c0==value1> && c0<=<value2>, value1>=value2
				if (opColumn[Query::Column::Equal] != NULL
						&& opColumn[Query::Column::Equal]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Equal]->value
							> curQueryOp->value) {
						return false;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}

				}

				//Check c0>=<value1> && c0<=<value2>, value1>value2
				if (opColumn[Query::Column::GreaterOrEqual] != NULL
						&& opColumn[Query::Column::GreaterOrEqual]->op
								!= Query::Column::Invalid) {

					if (opColumn[Query::Column::GreaterOrEqual]->value
							> curQueryOp->value) {
						return false;
					}
					//TODO: if(value1 == value2) replace with equal
					else if (opColumn[Query::Column::GreaterOrEqual]->value
							== curQueryOp->value) {
						opColumn[Query::Column::GreaterOrEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::GreaterOrEqual] = NULL;
						curQueryOp->op = Query::Column::Equal;
					}
				}

				//Check c0><value1> && c0<=<value2>, value1>=value2
				if (opColumn[Query::Column::Greater] != NULL
						&& opColumn[Query::Column::Greater]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Greater]->value
							> curQueryOp->value) {
						return false;
					}
				}

				//Check c0!=<value1> && c0<=<value2> value1<=value2
				if (opColumn[Query::Column::NotEqual] != NULL
						&& opColumn[Query::Column::NotEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::NotEqual]->value
							> curQueryOp->value) {
						//TODO: remove != from queries
						opColumn[Query::Column::NotEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::NotEqual] = NULL;

					}
				}

				//Check c0<=<value1> && c0<=<value2>
				if (opColumn[Query::Column::LessOrEqual] != NULL
						&& opColumn[Query::Column::LessOrEqual]->op
								!= Query::Column::Invalid
						&& opColumn[Query::Column::LessOrEqual] != curQueryOp) {
					//TODO: keep the smallest one from queries
					if (opColumn[Query::Column::LessOrEqual]->value
							>= curQueryOp->value) {
						opColumn[Query::Column::LessOrEqual]->op =
								Query::Column::Invalid;
						opColumn[Query::Column::LessOrEqual] = curQueryOp;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}
				}

				//Check c0<<value1> && c0<=<value2>
				if (opColumn[Query::Column::Less] != NULL
						&& opColumn[Query::Column::Less]->op
								!= Query::Column::Invalid) {
					//TODO: Keep the smallest
					if (opColumn[Query::Column::Less]->value
							> curQueryOp->value) {
						opColumn[Query::Column::Less]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::Less] = NULL;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}
				}

				break;
//				////////////////////////////////////////////////////////////////////////////////////////
			case Query::Column::Greater:
#ifdef DEBUG
				++Greater;
#endif
				//Check c0==value1> && c0><value2>, value1>=value2
				if (opColumn[Query::Column::Equal] != NULL
						&& opColumn[Query::Column::Equal]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Equal]->value
							<= curQueryOp->value) {
						return false;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}

				}

				//Check c0<=<value1> && c0><value2>
				if (opColumn[Query::Column::LessOrEqual] != NULL
						&& opColumn[Query::Column::LessOrEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::LessOrEqual]->value
							<= curQueryOp->value) {
						return false;
					}
				}

				//Check c0<<value1> && c0><value2>
				if (opColumn[Query::Column::Less] != NULL
						&& opColumn[Query::Column::Less]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Less]->value
							<= curQueryOp->value) {
						return false;
					}
				}
				//Check c0!=<value1> && c0><value2>, value1<=value2
				if (opColumn[Query::Column::NotEqual] != NULL
						&& opColumn[Query::Column::NotEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::NotEqual]->value
							<= curQueryOp->value) {
						//TODO: remove != from queries
						opColumn[Query::Column::NotEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::NotEqual] = NULL;

					}
				}

				//Check c0>=<value1> && c0><value2>
				if (opColumn[Query::Column::GreaterOrEqual] != NULL
						&& opColumn[Query::Column::GreaterOrEqual]->op
								!= Query::Column::Invalid) {
					//TODO: Keep the max
					if (opColumn[Query::Column::GreaterOrEqual]->value
							> curQueryOp->value) {
						curQueryOp->op = Query::Column::Invalid;
						break;
					} else {
						opColumn[Query::Column::GreaterOrEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::GreaterOrEqual] = NULL;
					}
				}

				//Check c0><value1> && c0><value2>
				if (opColumn[Query::Column::Greater] != NULL
						&& opColumn[Query::Column::Greater]->op
								!= Query::Column::Invalid
						&& opColumn[Query::Column::Greater] != curQueryOp) {
					//TODO: Keep the max
					if (opColumn[Query::Column::Greater]->value
							< curQueryOp->value) {
						opColumn[Query::Column::Greater]->op =
								Query::Column::Invalid;
						opColumn[Query::Column::Greater] = curQueryOp;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;

					}
				}

				break;
//				////////////////////////////////////////////////////////////////////////////////////////
			case Query::Column::GreaterOrEqual:
#ifdef DEBUG
				++GreaterOrEqual;
#endif
				//Check c0==value1> && c0>=<value2>, value1>value2
				if (opColumn[Query::Column::Equal] != NULL
						&& opColumn[Query::Column::Equal]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Equal]->value
							< curQueryOp->value) {
						return false;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}

				}
				//Check c0<=<value1> && c0><value2>
				if (opColumn[Query::Column::LessOrEqual] != NULL
						&& opColumn[Query::Column::LessOrEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::LessOrEqual]->value
							< curQueryOp->value) {
						return false;
					} else if (opColumn[Query::Column::LessOrEqual]->value
							== curQueryOp->value) {
						//TODO: values the same replace them with (==)
						opColumn[Query::Column::LessOrEqual]->op =
								Query::Column::Invalid;
						curQueryOp->op = Query::Column::Equal;

					}
				}

				//Check c0<<value1> && c0><value2>
				if (opColumn[Query::Column::Less] != NULL
						&& opColumn[Query::Column::Less]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::Less]->value
							<= curQueryOp->value) {
						return false;
					}
				}

				//Check c0!=<value1> && c0>=<value2>, value1<value2
				if (opColumn[Query::Column::NotEqual] != NULL
						&& opColumn[Query::Column::NotEqual]->op
								!= Query::Column::Invalid) {
					if (opColumn[Query::Column::NotEqual]->value
							< curQueryOp->value) {
						//TODO: remove != from queries and (<=) -> (<)
						opColumn[Query::Column::NotEqual]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::NotEqual] = NULL;

					}
				}

				//Check c0>=<value1> && c0><value2>
				if (opColumn[Query::Column::GreaterOrEqual] != NULL
						&& opColumn[Query::Column::GreaterOrEqual]->op
								!= Query::Column::Invalid
						&& opColumn[Query::Column::GreaterOrEqual]
								!= curQueryOp) {
					//TODO: Keep the max
					if (opColumn[Query::Column::GreaterOrEqual]->value
							< curQueryOp->value) {
						opColumn[Query::Column::GreaterOrEqual]->op =
								Query::Column::Invalid;
						opColumn[Query::Column::GreaterOrEqual] = curQueryOp;

					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}

				}

				//Check c0><value1> && c0><value2>
				if (opColumn[Query::Column::Greater] != NULL
						&& opColumn[Query::Column::Greater]->op
								!= Query::Column::Invalid) {
//					//TODO: Keep the max
					if (opColumn[Query::Column::Greater]->value
							< curQueryOp->value) {
						opColumn[Query::Column::Greater]->op =
								Query::Column::Invalid;
//						opColumn[Query::Column::Greater] = NULL;
					} else {
						curQueryOp->op = Query::Column::Invalid;
						break;
					}
				}

				break;

			case Query::Column::Invalid:
				continue;
			}

		}
	}
	return true;

}

inline static bool isGlobalQueryValid(Query* q) {
	//store end iteration
	for (unsigned i = 0; i < q->columnCount; i++) {
		auto curQueryOp = &(q->columns[i]);
		auto size = globalStats[q->relationId].size();
		if (size <= i) {
			auto& minmax = globalStats[q->relationId][i];
			switch (curQueryOp->op) {

			case Query::Column::Equal:
				if (curQueryOp->value > minmax[MAX]
						|| curQueryOp->value < minmax[MIN]) {
					return false;
				}
				break;

			case Query::Column::NotEqual:
				if (curQueryOp->value == minmax[MAX]
						&& minmax[MAX] == minmax[MIN]) {

					return false;
				}
				break;

			case Query::Column::Less:
				if (curQueryOp->value <= minmax[MIN]) {
					return false;
				}
				break;

			case Query::Column::LessOrEqual:

				if (curQueryOp->value < minmax[MIN])
					return false;
				break;

			case Query::Column::Greater:

				if (curQueryOp->value >= minmax[MAX])
					return false;
				break;
			case Query::Column::GreaterOrEqual:

				if (curQueryOp->value > minmax[MAX])
					return false;
				break;

			case Query::Column::Invalid:
				break;
			}
		}
	}
	return true;

}

inline static bool isLocalQueryValid(Query* q,
		unordered_map<uint32_t, vector<std::array<uint64_t, MINMAX>>>* rangedStatP) {
	auto& rangedStats = *rangedStatP;
	//store end iteration
	for (unsigned i = 0; i < q->columnCount; i++) {
		auto curQueryOp = &(q->columns[i]);

		//Ranged check
		auto size = rangedStats[q->relationId].size();
		if (size <= i) {
			auto& minmax = globalStats[q->relationId][i];
			switch (curQueryOp->op) {

			case Query::Column::Equal:
				if (curQueryOp->value > minmax[MAX]
						|| curQueryOp->value < minmax[MIN]) {
					return false;
				}
				break;

			case Query::Column::NotEqual:
				if (curQueryOp->value == minmax[MAX]
						&& minmax[MAX] == minmax[MIN]) {

					return false;
				}
				break;

			case Query::Column::Less:
				if (curQueryOp->value <= minmax[MIN]) {
					return false;
				}
				break;

			case Query::Column::LessOrEqual:

				if (curQueryOp->value < minmax[MIN])
					return false;
				break;

			case Query::Column::Greater:

				if (curQueryOp->value >= minmax[MAX])
					return false;
				break;
			case Query::Column::GreaterOrEqual:

				if (curQueryOp->value > minmax[MAX])
					return false;
				break;

			case Query::Column::Invalid:
				break;
			}
		}

	}
	return true;

}

#endif /* VALIDATION_H_ */
