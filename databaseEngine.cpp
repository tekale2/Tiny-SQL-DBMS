#include "databaseEngine.h"
using namespace std;


/*--------------Private Class Functions-------------*/
bool DatabaseEngine::execCreateQuery(Node *root)
{
	string table_name;
	vector<string> field_names;
  	vector<enum FIELD_TYPE> field_types;
	Relation* relation_ptr;
	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	getAttributeTypeList(root, field_names, field_types);

	// create a new schema
	Schema schema(field_names,field_types);
	relation_ptr = schema_manager.createRelation(table_name,schema);
	if(relation_ptr == NULL)
		return false;
	return true;
}

bool DatabaseEngine::execDropQuery(Node *root)
{
	string table_name;
	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	schema_manager.deleteRelation(table_name);
	return true;
}

bool DatabaseEngine::execInsertQuery(Node *root)
{
	return false;
}

bool DatabaseEngine::execDeleteQuery(Node *root)
{
	return false;
}

bool DatabaseEngine::execSelectQuery(Node *root)
{
	return false;
}

/*--------------Public Class Functions-------------*/

// Constructor to initilaize the databaseEngine with a memory and a disk
// This in turn initializes schema manager and buffer manager
DatabaseEngine::DatabaseEngine(MainMemory *mem, Disk *disk):schema_manager(mem,disk), buffer_manager(mem)
{
	this->mem = mem;
	this->disk = disk;
}

 
// logger function to log output to std out
// TODO: log to a file after all queries are implemented
void DatabaseEngine::log(string value)
{
	cout<<value<<endl;
	return;
}

// the main functions which takens in raw query as input
// returns"SUCCESS" for successful query execution
// returns "FAILED" if any the query execution fails at any point
string DatabaseEngine::execQuery(string query)
{
	Node *root;
	enum NODE_TYPE nodeType;
	string returnCode = "FAILED";
	
	// parse the query and create a parse tree

	root = parseQuery(query);
	if(root == NULL)
	{
		log("Error in parsing query:->   "+query);
		return returnCode;
	}

	// reset the disk ios and timers
	disk->resetDiskIOs();
    disk->resetDiskTimer();
	

    log("\n\n");
    log("Input Query:->  "+query);

    //execute the query
    nodeType = root->nodeType;
    switch(nodeType)
    {
    	case NODE_TYPE::CREATE_QUERY:
    		returnCode = execCreateQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::INSERT_QUERY:
    		returnCode = execInsertQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::DROP_QUERY:
    		returnCode = execDropQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::DELETE_QUERY:
    		returnCode = execDeleteQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::SELECT_QUERY:
    		returnCode = execSelectQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	default:
    	break;
    }

    // record disk IOs and 
    log("Disk I/O count: " + std::to_string(disk->getDiskIOs()));
    log("Query Exec Time: " + std::to_string(disk->getDiskTimer()) + " ms");
	delete root;
	return returnCode;
}