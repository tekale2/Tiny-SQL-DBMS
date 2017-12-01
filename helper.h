#ifndef _HELPER_H
#define _HELPER_H

#include <vector>
#include <string>
#include "parser.h"
#include "./StorageManager/Block.h"
#include "./StorageManager/Config.h"
#include "./StorageManager/Disk.h"
#include "./StorageManager/Field.h"
#include "./StorageManager/MainMemory.h"
#include "./StorageManager/Relation.h"
#include "./StorageManager/Schema.h"
#include "./StorageManager/SchemaManager.h"
#include "./StorageManager/Tuple.h"
#include "bufferManager.h"

using namespace std;

// Appends a tuple to the end of a memory block (taken from TestStorageManager.cpp)
// using memory block "memory_block_index" as destination buffer
bool appendTupleToMemory(MainMemory *mem, int memory_block_index, Tuple& tuple);

// Appends a tuple to the end of a relation (taken from TestStorageManager.cpp)
// using memory block "memory_block_index" as output buffer
void appendTupleToRelation(Relation* relation_ptr, MainMemory *mem, int memory_block_index, Tuple& tuple);

// Performs onepass sort in memory
void onePassSort(MainMemory *mem, vector<int> &memBlockIndices, string &colName, enum FIELD_TYPE field_type);

// Performs two pass sort
void twoPassSort(Relation *rel, Relation *temp, Relation *output, MainMemory *mem, BufferManager &buffer_manager,\
	string &colName,enum FIELD_TYPE field_type);

// Performs duplicate elmination in memory
void onePassRemoveDups(MainMemory *mem,vector<int> &memBlockIndices, BufferManager &buffer_manager,\
	string &colName,enum FIELD_TYPE field_type);

// contains helper functions for the query execution
/* returns pointer to the required Node Type
 returns Null if not found*/
Node* getNode(enum NODE_TYPE nodeType, Node *root);

// returns true if there is a node of given nodeType
bool hasNode(enum NODE_TYPE nodeType, Node *root);

// searches and returns the value of first instance of specified NodeType
string getNodeVal(enum NODE_TYPE nodeType, Node *root);

// loads the vectors with dataTypes and columnsNames
void getAttributeTypeList(Node *root, vector<string>& field_names, vector<enum FIELD_TYPE>& field_types);

// Returns a vector of values for multiple instances of specified node type in the right order
vector<string> getNodeTypeLists(enum NODE_TYPE nodeType, Node *root);

// gets and returns the column name of the orderby column
string getOrdeByColumnName(Node *root);

#endif