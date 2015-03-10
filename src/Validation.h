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
	for (unsigned i = 0; i < q->columnCount; i++) {
		auto curQueryOp = &(q->columns[i]);
		//for each column check
		auto t = opsMap.find(curQueryOp->column);
		if (t == opsMap.end()) {
			opsMap[curQueryOp->column][curQueryOp->op] = curQueryOp;
		} else {
			//Get the column from the columns map
			std::array<Query::Column*, OPERATORS> opColumn = t->second;

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

#endif /* VALIDATION_H_ */
