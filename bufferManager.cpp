#include <algorithm>
#include <functional>
#include "bufferManager.h"

// constructor to initilaize the buffer manager
BufferManager::BufferManager( MainMemory *mem)
{
	int size;
	this->mem = mem;
	size = mem->getMemorySize();
	for(int i = size-1;i>=0;i--)
			freeblockList.push_back(i);
}
// get a free memory block index
int BufferManager::getFreeBlockIdx()
{
	int idx;
	Block *temp;
	if(freeblockList.empty())
		return -1;
	idx = freeblockList.back();
	freeblockList.pop_back();
	temp = mem->getBlock(idx);
	temp->clear();
	return idx;
}

// store back the memory block index
void BufferManager::storeFreeBlockIdx(int idx)
{
	freeblockList.push_back(idx);
}

//get freeBlockCount
int BufferManager::getFreeBlocksCount()
{
	return freeblockList.size();
}

// sort the indices in decreasing order
void BufferManager::sort()
{
	std::sort(freeblockList.begin(), freeblockList.end(),std::greater<int>());
	return;
}

// release list of blocks
void BufferManager::releaseBulkIdx(vector<int> &indices)
{
	for(int i:indices)
		storeFreeBlockIdx(i);
	return;
}