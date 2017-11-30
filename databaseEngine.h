#ifndef _DATABASEENGINE_H
#define _DATABASEENGINE_H

#include <string>
#include <iostream>

#include "./StorageManager/Block.h"
#include "./StorageManager/Config.h"
#include "./StorageManager/Disk.h"
#include "./StorageManager/Field.h"
#include "./StorageManager/MainMemory.h"
#include "./StorageManager/Relation.h"
#include "./StorageManager/Schema.h"
#include "./StorageManager/SchemaManager.h"
#include "./StorageManager/Tuple.h"

#include "parser.h"
#include "bufferManager.h"
#include "helper.h"
#include "whereEval.h"

using namespace std;

class DatabaseEngine
{
private:
	MainMemory *mem;
	Disk *disk;
	SchemaManager schema_manager;
	BufferManager buffer_manager;

	// for holding the state for selectStatement
	vector<int> memoryBlockIndices;
	bool resultInMemory;
	vector<Relation*> tempRelations;

	bool execCreateQuery(Node *root);
	bool execDropQuery(Node *root);
	bool execInsertQuery(Node *root);
	bool execDeleteQuery(Node *root);
	Relation* execSelectQuery(Node *root, bool doLog);
	Relation* execOrderBy(Relation *rel, string colName);
	void cleanUp();

	// returns the pointer of temporary relation
	Relation* tableScan(string &tableName, vector<string> &selectList, string whereCond);
	// logger functions to log output
	void log(Relation *relation_ptr);
	void log(Schema *schema);
	void log(Tuple &tuple);
	void log(string value);

public:

	DatabaseEngine(MainMemory *mem, Disk *disk);

	// the main functions which takens in raw query as input
	void execQuery(string query);

	
};

#endif
