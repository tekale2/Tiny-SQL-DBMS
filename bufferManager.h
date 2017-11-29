#ifndef _BUFFERMANAGER_H
#define _BUFFERMANAGER_H

#include <vector>
#include <string>
#include "./StorageManager/Block.h"
#include "./StorageManager/Config.h"
#include "./StorageManager/Disk.h"
#include "./StorageManager/Field.h"
#include "./StorageManager/MainMemory.h"
#include "./StorageManager/Relation.h"
#include "./StorageManager/Schema.h"
#include "./StorageManager/SchemaManager.h"
#include "./StorageManager/Tuple.h"

using namespace std;

class BufferManager
{
private:
	vector<int> freeblockList;
	MainMemory *mem;
public:
	// constructor to initilaize the buffer manager
	BufferManager( MainMemory *mem);
	// get a free memory block index
	int getFreeBlockIdx();

	// get freeBlockCount
	int getFreeBlocksCount();

	// sort the indices in increasing order
	void sort();
	
	// store back the memory block index
	void storeFreeBlockIdx(int idx);

	// release list of blocks
	void releaseBulkIdx(vector<int> &indices);
	
};

#endif