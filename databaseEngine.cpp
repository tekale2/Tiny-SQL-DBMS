#include <unordered_map>
#include "databaseEngine.h"

using namespace std;

/*--------------Private Functions-------------------*/

// Appends a tuple to the end of a relation (taken from TestStorageManager.cpp)
// using memory block "memory_block_index" as output buffer
static void appendTupleToRelation(Relation* relation_ptr, MainMemory *mem, int memory_block_index, Tuple& tuple) 
{
  Block* block_ptr;
  if (relation_ptr->getNumOfBlocks()==0) {
    block_ptr=mem->getBlock(memory_block_index);
    block_ptr->clear(); //clear the block
    block_ptr->appendTuple(tuple); // append the tuple
    relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index);
  } else {
    relation_ptr->getBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index);
    block_ptr=mem->getBlock(memory_block_index);

    if (block_ptr->isFull()) {
      block_ptr->clear(); //clear the block
      block_ptr->appendTuple(tuple); // append the tuple
      relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index); //write back to the relation
    } else {
      block_ptr->appendTuple(tuple); // append the tuple
      relation_ptr->setBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index); //write back to the relation
    }
  }  
}

// Converts a given schema to a map
static void schemaToMap(unordered_map<string,enum FIELD_TYPE> &colType, Schema &schema)
{
	vector<string>  field_names;
	vector<enum FIELD_TYPE> field_types;

	field_names = schema.getFieldNames();
	field_types = schema.getFieldTypes();
	for(int i = 0;i<field_types.size();i++)
		colType[field_names[i]] = field_types[i];
}

// Converts a given tuple to a column map
static void tupleToMap(Schema &schema, Tuple &tuple, unordered_map<string, Field> &colValues)
{
	vector<string>  field_names;
	field_names = schema.getFieldNames();
	for(int i = 0;i<field_names.size();i++)
	{
		colValues[field_names[i]] = tuple.getField(field_names[i]);
	}
	return;
}
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
	string table_name;
	vector<string> values, attrList;
	int memIdx,value;
	bool validTuple = true;
	enum FIELD_TYPE field_type;
	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	Relation* relation_ptr;
	relation_ptr = schema_manager.getRelation(table_name);

	if(relation_ptr == NULL)
		return false;

	if( hasNode(NODE_TYPE::VALUES, root))
	{
		values = getNodeTypeLists(NODE_TYPE::VALUE_LIST,root);
		attrList = getNodeTypeLists(NODE_TYPE::ATTRIBUTE_LIST,root);
		Tuple tuple =  relation_ptr->createTuple();
		Schema schema = relation_ptr->getSchema();

		// make tuple
		for(int i = 0; i<values.size();i++)
		{
			field_type = schema.getFieldType(attrList[i]);
			if(field_type == FIELD_TYPE::INT)
			{
				try
				{
					value = stoi(values[i]);
				}
				catch(...)
				{
					if(values[i] == "NULL")
						value =0;
					else
					{
						log("Exception occurred when coverting string to int");
						return false;
					}
				}
				validTuple = tuple.setField(attrList[i],value);
			}
			else
			{
				validTuple = tuple.setField(attrList[i],values[i]);
			}
			if(!validTuple)
				return false;
		}

		//get a free mem block and append
		memIdx = buffer_manager.getFreeBlockIdx();
		if(memIdx == -1)
		{
			log("Out of free memory blocks");
			return false;
		}

		appendTupleToRelation(relation_ptr, mem, memIdx, tuple);
		buffer_manager.storeFreeBlockIdx(memIdx);
	  	cout << "Now the relation contains: " << endl;
	  	cout << *relation_ptr << endl << endl;
	}
	else
	{
		// TODO: Implement after select
		return false;
	}



	return true;
}

bool DatabaseEngine::execDeleteQuery(Node *root)
{
	unordered_map<string,enum FIELD_TYPE> colType;
	unordered_map<string, Field> colValues;
	string table_name, whereString;
	CondEval whereCondEval;
	vector<Tuple> tuples;
	int memInIdx, memOutIdx, numBlocks, writeIdx;
	Block *in;
	Block *out;
	Relation* relation_ptr;
	

	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	relation_ptr = schema_manager.getRelation(table_name);
	if(relation_ptr == NULL)
		return false;

	if( !hasNode(NODE_TYPE::WHERE, root))
	{
		relation_ptr->deleteBlocks(0);
      	return true;
	}
  	cout << "Before Delete the relation contains: " << endl;
  	cout << *relation_ptr << endl << endl;
	//intilize whereCond
	Schema schema = relation_ptr->getSchema();
	schemaToMap(colType, schema);

	whereString =  getNodeVal(NODE_TYPE::CONDITION_STR,root);
	whereCondEval.intialize(whereString,colType);

	// get 2 memory buffers
	memInIdx = buffer_manager.getFreeBlockIdx();
	memOutIdx = buffer_manager.getFreeBlockIdx();
	out = mem->getBlock(memOutIdx);
	numBlocks = relation_ptr->getNumOfBlocks();

	
	writeIdx = 0;

	for(int i = 0; i<numBlocks;i++)
	{
		// load block into 1 buffer
		relation_ptr->getBlock(i,memInIdx);
		in = mem->getBlock(memInIdx);
		tuples = in->getTuples();
		for(Tuple &t:tuples)
		{
			tupleToMap(schema, t, colValues);
			if(whereCondEval.evaluate(colValues))
				continue;
		    if (out->isFull()) 
		    {
				relation_ptr->setBlock(writeIdx,memOutIdx); //write back to the relation
				out->clear(); //clear the block
				out->appendTuple(t); // append the tuple
				writeIdx++;
		    } else {
		    	out->appendTuple(t); // append the tuple
		    }
		}
	}
	if(!(out->isEmpty()))
	{
		relation_ptr->setBlock(writeIdx,memOutIdx); //write back to the relation
		out->clear(); //clear the block
		writeIdx++;
	}
	relation_ptr->deleteBlocks(writeIdx);

	buffer_manager.storeFreeBlockIdx(memOutIdx);
	buffer_manager.storeFreeBlockIdx(memInIdx);

  	cout << "After Delete the relation contains: " << endl;
  	cout << *relation_ptr << endl << endl;

	return true;
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
		log("Error in parsing query:->"+query);
		return returnCode;
	}

	// reset the disk ios and timers
	disk->resetDiskIOs();
    disk->resetDiskTimer();
	

    log("\n");
    log("************************************************************");
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
    log("************************************************************");
    log("\n");
	delete root;
	return returnCode;
}