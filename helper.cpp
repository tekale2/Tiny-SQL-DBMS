#include "helper.h"
#include <queue>
#include <vector>
#include <algorithm>
using namespace std;


/*------------------------PQP Algorithm functions----------------------------*/

/*--------------Structure to support one Pass Sorting----*/

struct TupleCmp
{
	private:
		enum FIELD_TYPE field_type;
		string columnName;
	public:
		TupleCmp(enum FIELD_TYPE f, string cname):field_type(f),columnName(cname)
		{}
		bool operator ()(const Tuple &t1, const Tuple &t2)
		{
			bool result = false;
			if(field_type == FIELD_TYPE::INT)
				result = t1.getField(columnName).integer < t2.getField(columnName).integer;
			else
				result = *(t1.getField(columnName).str) < *(t2.getField(columnName).str);
			return result;

		}
};

/*--------------Structure to support 2 pass Sort Heap ------*/


static bool tuplesEqual(Tuple &t1, Tuple &t2)
{
	Schema schema = t1.getSchema();
	vector<enum FIELD_TYPE> field_types = schema.getFieldTypes();
	for(int i = 0; i<field_types.size();i++)
	{
		if(field_types[i] == FIELD_TYPE::INT)
		{
			if(t1.getField(i).integer != t2.getField(i).integer)
				return false;
		}
		else
		{

			if(*(t1.getField(i).str) != *(t2.getField(i).str))
				return false;
		}
	}
	return true;
}

// Appends a tuple to the end of a memory block (taken from TestStorageManager.cpp)
// using memory block "memory_block_index" as destination buffer
bool appendTupleToMemory(MainMemory *mem, int memory_block_index, Tuple& tuple) 
{
	Block* block_ptr;
	block_ptr = mem->getBlock(memory_block_index);
	if(block_ptr->isFull())
		return false;
	block_ptr->appendTuple(tuple);
	return true;
}


// Appends a tuple to the end of a relation (taken from TestStorageManager.cpp)
// using memory block "memory_block_index" as output buffer
void appendTupleToRelation(Relation* relation_ptr, MainMemory *mem, int memory_block_index, Tuple& tuple) 
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
// Performs onepass sort in main memory
void onePassSort(MainMemory *mem, vector<int> &memBlockIndices, string &colName, enum FIELD_TYPE field_type)
{
	vector<Tuple> tuples;
	tuples = mem->getTuples(memBlockIndices[0],memBlockIndices.size());
	sort(tuples.begin(),tuples.end(),TupleCmp(field_type,colName));
	mem->setTuples(memBlockIndices[0],tuples);
	return;
}


// Performs duplicate elmination in memory
void onePassRemoveDups(MainMemory *mem,vector<int> &memBlockIndices, BufferManager &buffer_manager,\
	string &colName,enum FIELD_TYPE field_type)
{
	// get tuples and sort
	vector<Tuple> tuples, output;
	int i = 0;
	int freeMemIdx;
	tuples = mem->getTuples(memBlockIndices[0],memBlockIndices.size());
	sort(tuples.begin(),tuples.end(),TupleCmp(field_type,colName));	

	// remove duplicates from vector:
    for(int j = 1;j<tuples.size();j++)
    {
        if(!tuplesEqual(tuples[i],tuples[j]))
        {
            i++;
            tuples[i]  = tuples[j];
        }
    }
    for(int j =0;j<=(i);j++)
    {
    	output.push_back(tuples[j]);
    }
    
    // reload the tuples into memory
    buffer_manager.releaseBulkIdx(memBlockIndices);
	memBlockIndices.clear();
	buffer_manager.sort();

	freeMemIdx = buffer_manager.getFreeBlockIdx();
	memBlockIndices.push_back(freeMemIdx);
	for(Tuple &t:output)
	{
		if (!appendTupleToMemory(mem, freeMemIdx,t))
		{
				freeMemIdx = buffer_manager.getFreeBlockIdx();
				memBlockIndices.push_back(freeMemIdx);
				appendTupleToMemory(mem, freeMemIdx,t);
		}
	}

	return;
}

/*-----------------------LQP Helper functions----------------------------------*/
/* returns pointer to the required Node Type
 returns Null if not found*/
Node* getNode(enum NODE_TYPE nodeType, Node *root)
{
	queue<Node*> nodeQ;
	Node *temp;
	Node *found = NULL;
	nodeQ.push(root);
	while(!nodeQ.empty())
	{
		temp = nodeQ.front();
		nodeQ.pop();
		if(temp->nodeType == nodeType)
		{
			found = temp;
			break;
		}
		for(int i = 0;i<temp->children.size();i++)
			nodeQ.push(temp->children[i]);
	}
	return found;
}

// returns true if there is a node of given nodeType
bool hasNode(enum NODE_TYPE nodeType, Node *root)
{
	queue<Node*> nodeQ;
	Node *temp;
	bool found = false;
	nodeQ.push(root);
	while(!nodeQ.empty())
	{
		temp = nodeQ.front();
		nodeQ.pop();
		if(temp->nodeType == nodeType)
		{
			found = true;
			break;
		}
		for(int i = 0;i<temp->children.size();i++)
			nodeQ.push(temp->children[i]);
	}
	return found;
}

// gets and returns the column name of the orderby column
string getOrdeByColumnName(Node *root)
{
	int i = 0;
	for( i = 0;i<root->children.size();i++)
		if(root->children[i]->nodeType == NODE_TYPE::ORDER)
			break;
	return root->children[i+2]->nodeVal;

}
// searches and returns the value of first instance of specified NodeType
string getNodeVal(enum NODE_TYPE nodeType, Node *root)
{
	queue<Node*> nodeQ;
	string result;
	Node *temp;
	Node *found = NULL;
	nodeQ.push(root);
	while(!nodeQ.empty())
	{
		temp = nodeQ.front();
		nodeQ.pop();
		if(temp->nodeType == nodeType)
		{
			found = temp;
			break;
		}
		for(int i = 0;i<temp->children.size();i++)
			nodeQ.push(temp->children[i]);
	}
	result = (found==NULL)? "Not Found":found->nodeVal;
	return result;	
}

// loads the vectors with dataTypes and columnsNames
void getAttributeTypeList(Node *root, vector<string>& field_names, vector<enum FIELD_TYPE>& field_types)
{
	Node *attrRoot = getNode(NODE_TYPE::ATTRIBUTE_TYPE_LIST,root);
	Node *curr;
	enum FIELD_TYPE f;
	curr = attrRoot;
	if(curr==NULL)
		return;
	while(curr->children.size()>=2)
	{
		field_names.push_back(curr->children[0]->nodeVal);
		f = (curr->children[1]->nodeVal == "INT")? (FIELD_TYPE::INT): (FIELD_TYPE::STR20);
		field_types.push_back(f);
		if(curr->children.size() == 2)
			break;
		else
			curr = curr->children[2];

	}
	return;
}

// Returns a vector of values for multiple instances of specified node type in the right order
vector<string> getNodeTypeLists(enum NODE_TYPE nodeType, Node *root)
{
	vector<string> result;
	Node *curr = getNode(nodeType,root);
	if(curr==NULL)
		return result;
	while(curr->children.size()>=1)
	{
		result.push_back(curr->children[0]->nodeVal);
		if(curr->children.size() == 1)
			break;
		else
			curr = curr->children[1];
	}
	return result;
}