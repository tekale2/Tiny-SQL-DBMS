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

using namespace std;

class DatabaseEngine
{
private:
	MainMemory *mem;
	Disk *disk;
	SchemaManager schManager;
	BufferManager bManager;

	bool execCreateQuery(Node *root);
	bool execDropQuery(Node *root);
	bool execInsertQuery(Node *root);

public:

	DatabaseEngine(MainMemory *mem, Disk *disk);

	// logger function to log output
	void log(string type, string value);
	// the main functions which takens in raw query as input
	bool execQuery(string query);

	
};

#endif
