#include "bufferManager.h"

// constructor to initilaize the buffer manager
BufferManager::BufferManager( MainMemory *mem)
{
	int size;
	this->mem = mem;
	size = mem->getMemorySize();
	for(int i = size-1;i>=0;i--)
			freeblockList.push(i);
}
// get a free memory block index
int BufferManager::getFreeBlockIdx()
{
	int idx;
	Block *temp;
	if(freeblockList.empty())
		return -1;
	idx = freeblockList.top();
	freeblockList.pop();
	temp = mem->getBlock(idx);
	temp->clear();
	return idx;
}
// store back the memory block index
void BufferManager::storeFreeBlockIdx(int idx)
{
	freeblockList.push(idx);
}