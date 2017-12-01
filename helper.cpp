#include "helper.h"
#include <queue>
#include <algorithm>
#include <functional>
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

struct HeapElement
{

	int integer;
	string str;
	int currMemIdx;
	int tupleIdx;
	enum FIELD_TYPE field_type;
	bool operator > (const HeapElement &h1) const
	{
		bool result = false;
		if(field_type == FIELD_TYPE::INT)
			result = integer > h1.integer;
		else
			result = str > h1.str;
		return result;	
	}
};

struct QElement
{
	int diskBlockIdx;
	int nextTupleIdx;
	int numTuplesInBlock;
};
/*-------------------------------------------------------------*/


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


// Performs 2 pass sorting and returns new Relation
void twoPassSort(Relation *rel, Relation *temp, Relation *output, MainMemory *mem, BufferManager &buffer_manager,\
	string &colName,enum FIELD_TYPE field_type)
{
	vector<queue<QElement>> subList;
	priority_queue<HeapElement, vector<HeapElement>, greater<HeapElement> > minHeap;
	QElement qElement;
	HeapElement heapElement;
	int numBlocks, freeMemBlocks, subListSize;
	int startDiskIdx, endDiskIdx, k, outputIdx, currDiskBlock;
	vector<int> freeMemBlockIndices, used;
	Block *blkPtr;
	vector<Tuple> tuples;
	numBlocks = rel->getNumOfBlocks();
	freeMemBlocks = buffer_manager.getFreeBlocksCount();
	subListSize = (numBlocks-1)/freeMemBlocks + 1;
	subList.resize(subListSize);
	buffer_manager.getAllFreeBlocks(freeMemBlockIndices);
	
	// First Pass: sort the relations and store it back to the disk in new relation
	for(int i = 0; i<subListSize;i++)
	{
		startDiskIdx = i*freeMemBlocks;
		endDiskIdx = startDiskIdx+ freeMemBlocks -1;
		if(endDiskIdx > (numBlocks-1))
			endDiskIdx = numBlocks-1;
		k =0;
		for(int j =startDiskIdx;j<=endDiskIdx;j++)
		{
			rel->getBlock(j, k);
			k++;
		}
		
		used = freeMemBlockIndices;
		used.resize((endDiskIdx - startDiskIdx + 1));
		// perform one pass sort
		onePassSort(mem, used, colName,field_type);
		
		// store back the result
		k =0;
		for(int j =startDiskIdx;j<=endDiskIdx;j++)
		{
			qElement.diskBlockIdx = j;
			qElement.nextTupleIdx = 0;
			blkPtr = mem->getBlock(k);
			qElement.numTuplesInBlock = blkPtr->getNumTuples();
			subList[i].push(qElement);
			tuples = blkPtr->getTuples();
			for(Tuple &t:tuples)
				appendTupleToRelation(temp, mem, k, t);
			k++;
		}
	}
	// clear memory
	buffer_manager.releaseBulkIdx(freeMemBlockIndices);
	buffer_manager.sort();
	buffer_manager.getAllFreeBlocks(freeMemBlockIndices);
	outputIdx = freeMemBlockIndices.back();
	freeMemBlockIndices.pop_back();

	//Build heap
	for(int i = 0; i<subListSize;i++)
	{
		// bring first block of each sublist to memory
		qElement = subList[i].front();
		temp->getBlock(qElement.diskBlockIdx, i);
		blkPtr = mem->getBlock(i);
		Tuple temp = blkPtr->getTuple(0);
		
		if(field_type == FIELD_TYPE::INT)
			heapElement.integer = temp.getField(colName).integer;
		else
			heapElement.str = *(temp.getField(colName).str);
		heapElement.tupleIdx = 0;
		heapElement.currMemIdx = i;
		heapElement.field_type = field_type;
		minHeap.push(heapElement);
		subList[i].front().nextTupleIdx = 1;
	}

	currDiskBlock = 0;
	
	//2nd pass store the sorted result
	while(!minHeap.empty())
	{
		heapElement = minHeap.top();
		minHeap.pop();
		// Store the tuple into relation
		blkPtr = mem->getBlock(heapElement.currMemIdx);
		Tuple tuple = blkPtr->getTuple(heapElement.tupleIdx);

		if(!appendTupleToMemory(mem, outputIdx,tuple))
		{
			output->setBlock(currDiskBlock,outputIdx);
			currDiskBlock++;
			blkPtr = mem->getBlock(outputIdx);
			blkPtr->clear();
			appendTupleToMemory(mem, outputIdx,tuple);
		}
		// load another tuple from the sublist
		if(!subList[heapElement.currMemIdx].empty())
		{
			qElement = subList[heapElement.currMemIdx].front();
			if(qElement.nextTupleIdx < qElement.numTuplesInBlock)
			{
				blkPtr = mem->getBlock(heapElement.currMemIdx);
				Tuple temp = blkPtr->getTuple(qElement.nextTupleIdx);
				if(field_type == FIELD_TYPE::INT)
					heapElement.integer = temp.getField(colName).integer;
				else
					heapElement.str = *(temp.getField(colName).str);
				heapElement.currMemIdx = heapElement.currMemIdx;
				heapElement.tupleIdx = qElement.nextTupleIdx;
				heapElement.field_type = field_type;
				minHeap.push(heapElement);
				qElement.nextTupleIdx++;
				subList[heapElement.currMemIdx].front().nextTupleIdx = qElement.nextTupleIdx;
			}
			else
			{
				subList[heapElement.currMemIdx].pop();
				if(!subList[heapElement.currMemIdx].empty())
				{
					qElement = subList[heapElement.currMemIdx].front();
					// get the block in memory
					temp->getBlock(qElement.diskBlockIdx,heapElement.currMemIdx);
					blkPtr = mem->getBlock(heapElement.currMemIdx);
					Tuple temp = blkPtr->getTuple(0);
					if(field_type == FIELD_TYPE::INT)
						heapElement.integer = temp.getField(colName).integer;
					else
						heapElement.str = *(temp.getField(colName).str);
					heapElement.currMemIdx = heapElement.currMemIdx;
					heapElement.tupleIdx = 0;
					heapElement.field_type = field_type;
					minHeap.push(heapElement);
				    subList[heapElement.currMemIdx].front().nextTupleIdx = 1;
				}
			}

		}
	}
	output->setBlock(currDiskBlock,outputIdx);

	// clear any used memory and return
	freeMemBlockIndices.push_back(outputIdx);
	buffer_manager.releaseBulkIdx(freeMemBlockIndices);
	buffer_manager.sort();

	return;
}

void twoPassRemoveDups(Relation *rel, Relation *temp, Relation *output, MainMemory *mem, BufferManager &buffer_manager,\
	string &colName,enum FIELD_TYPE field_type)
{
	vector<queue<QElement>> subList;
	priority_queue<HeapElement, vector<HeapElement>, greater<HeapElement> > minHeap;
	QElement qElement;
	HeapElement heapElement;
	bool append;
	int numBlocks, freeMemBlocks, subListSize;
	int startDiskIdx, endDiskIdx, k, outputIdx, currDiskBlock;
	vector<int> freeMemBlockIndices, used;
	Block *blkPtr;
	vector<Tuple> tuples;
	vector<Tuple> lastInserted;
	numBlocks = rel->getNumOfBlocks();
	freeMemBlocks = buffer_manager.getFreeBlocksCount();
	subListSize = (numBlocks-1)/freeMemBlocks + 1;
	subList.resize(subListSize);
	buffer_manager.getAllFreeBlocks(freeMemBlockIndices);
	
	// First Pass: sort the relations and store it back to the disk in new relation
	for(int i = 0; i<subListSize;i++)
	{
		startDiskIdx = i*freeMemBlocks;
		endDiskIdx = startDiskIdx+ freeMemBlocks -1;
		if(endDiskIdx > (numBlocks-1))
			endDiskIdx = numBlocks-1;
		k =0;
		for(int j =startDiskIdx;j<=endDiskIdx;j++)
		{
			rel->getBlock(j, k);
			k++;
		}
		
		used = freeMemBlockIndices;
		used.resize((endDiskIdx - startDiskIdx + 1));
		// perform one pass sort
		onePassSort(mem, used, colName,field_type);
		
		// store back the result
		k =0;
		for(int j =startDiskIdx;j<=endDiskIdx;j++)
		{
			qElement.diskBlockIdx = j;
			qElement.nextTupleIdx = 0;
			blkPtr = mem->getBlock(k);
			qElement.numTuplesInBlock = blkPtr->getNumTuples();
			subList[i].push(qElement);
			tuples = blkPtr->getTuples();
			for(Tuple &t:tuples)
				appendTupleToRelation(temp, mem, k, t);
			k++;
		}
	}
	// clear memory
	buffer_manager.releaseBulkIdx(freeMemBlockIndices);
	buffer_manager.sort();
	buffer_manager.getAllFreeBlocks(freeMemBlockIndices);
	outputIdx = freeMemBlockIndices.back();
	freeMemBlockIndices.pop_back();

	//Build heap
	for(int i = 0; i<subListSize;i++)
	{
		// bring first block of each sublist to memory
		qElement = subList[i].front();
		temp->getBlock(qElement.diskBlockIdx, i);
		blkPtr = mem->getBlock(i);
		Tuple temp = blkPtr->getTuple(0);
		
		if(field_type == FIELD_TYPE::INT)
			heapElement.integer = temp.getField(colName).integer;
		else
			heapElement.str = *(temp.getField(colName).str);
		heapElement.tupleIdx = 0;
		heapElement.currMemIdx = i;
		heapElement.field_type = field_type;
		minHeap.push(heapElement);
		subList[i].front().nextTupleIdx = 1;
	}

	currDiskBlock = 0;
	
	//2nd pass store the sorted result
	while(!minHeap.empty())
	{
		heapElement = minHeap.top();
		minHeap.pop();
		// Store the tuple into relation
		blkPtr = mem->getBlock(heapElement.currMemIdx);
		Tuple tuple = blkPtr->getTuple(heapElement.tupleIdx);
		append = false;

		if(lastInserted.size() == 0)
		{
			append = true;
			lastInserted.push_back(tuple);
		}
		else if(!tuplesEqual(tuple,lastInserted[0]))
		{
			lastInserted[0] = tuple;
			append = true;
		}
		if(append)
		{
			if(!appendTupleToMemory(mem, outputIdx,tuple))
			{
				output->setBlock(currDiskBlock,outputIdx);
				currDiskBlock++;
				blkPtr = mem->getBlock(outputIdx);
				blkPtr->clear();
				appendTupleToMemory(mem, outputIdx,tuple);
			}
		}

		// load another tuple from the sublist
		if(!subList[heapElement.currMemIdx].empty())
		{
			qElement = subList[heapElement.currMemIdx].front();
			if(qElement.nextTupleIdx < qElement.numTuplesInBlock)
			{
				blkPtr = mem->getBlock(heapElement.currMemIdx);
				Tuple temp = blkPtr->getTuple(qElement.nextTupleIdx);
				if(field_type == FIELD_TYPE::INT)
					heapElement.integer = temp.getField(colName).integer;
				else
					heapElement.str = *(temp.getField(colName).str);
				heapElement.currMemIdx = heapElement.currMemIdx;
				heapElement.tupleIdx = qElement.nextTupleIdx;
				heapElement.field_type = field_type;
				minHeap.push(heapElement);
				qElement.nextTupleIdx++;
				subList[heapElement.currMemIdx].front().nextTupleIdx = qElement.nextTupleIdx;
			}
			else
			{
				subList[heapElement.currMemIdx].pop();
				if(!subList[heapElement.currMemIdx].empty())
				{
					qElement = subList[heapElement.currMemIdx].front();
					// get the block in memory
					temp->getBlock(qElement.diskBlockIdx,heapElement.currMemIdx);
					blkPtr = mem->getBlock(heapElement.currMemIdx);
					Tuple temp = blkPtr->getTuple(0);
					if(field_type == FIELD_TYPE::INT)
						heapElement.integer = temp.getField(colName).integer;
					else
						heapElement.str = *(temp.getField(colName).str);
					heapElement.currMemIdx = heapElement.currMemIdx;
					heapElement.tupleIdx = 0;
					heapElement.field_type = field_type;
					minHeap.push(heapElement);
				    subList[heapElement.currMemIdx].front().nextTupleIdx = 1;
				}
			}

		}
	}
	output->setBlock(currDiskBlock,outputIdx);

	// clear any used memory and return
	freeMemBlockIndices.push_back(outputIdx);
	buffer_manager.releaseBulkIdx(freeMemBlockIndices);
	buffer_manager.sort();

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
string getOrderByColumnName(Node *root)
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