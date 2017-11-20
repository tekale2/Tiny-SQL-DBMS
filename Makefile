cc = g++ -std=c++11

all: TinySql
	rm *.o

TinySql: StorageManager.o databaseEngine.o bufferManager.o parser.o helper.o
	$(cc) -o TinySql StorageManager.o databaseEngine.o bufferManager.o parser.o helper.o main.cpp

databaseEngine.o: databaseEngine.h
	$(cc) -c databaseEngine.cpp

bufferManager.o: bufferManager.h
	$(cc) -c bufferManager.cpp

StorageManager.o: StorageManager/Block.h StorageManager/Disk.h StorageManager/Field.h StorageManager/MainMemory.h StorageManager/Relation.h StorageManager/Schema.h StorageManager/SchemaManager.h StorageManager/Tuple.h StorageManager/Config.h
	$(cc) -c  StorageManager/StorageManager.cpp

parser.o: parser.h
	$(cc) -c parser.cpp

helper.o: helper.h
	$(cc) -c helper.cpp

clean:
	rm -f TinySql