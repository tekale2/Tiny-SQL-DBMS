#include "databaseEngine.h"
using namespace std;


bool DatabaseEngine::execCreateQuery(Node *root)
{
	return false;
}

bool DatabaseEngine::execDropQuery(Node *root)
{
	return false;
}

bool DatabaseEngine::execInsertQuery(Node *root)
{
	return false;
}


DatabaseEngine::DatabaseEngine(MainMemory *mem, Disk *disk):schManager(mem,disk), bManager(mem)
{
	this->mem = mem;
	this->disk = disk;
}


// logger function to log output
void DatabaseEngine::log(string type, string value)
{
	cout<<type<<" : "<<value<<endl;
	return;
}

// the main functions which takens in raw query as input
bool DatabaseEngine::execQuery(string query)
{
	log("query",query);
	return false;
}