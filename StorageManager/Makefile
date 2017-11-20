all: TestStorageManager

TestStorageManager: StorageManager.o 
	g++ -std=c++11 -o TestStorageManager StorageManager.o TestStorageManager.cpp

StorageManager.o: Block.h Disk.h Field.h MainMemory.h Relation.h Schema.h SchemaManager.h Tuple.h Config.h
	g++ -std=c++11 -c StorageManager.cpp

clean:
	rm *.o TestStorageManager
