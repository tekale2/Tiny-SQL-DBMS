#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include "databaseEngine.h"

using namespace std;

int main(int argc, char *argv[])
{
	MainMemory mem;
	string query;
	Disk disk;
	ifstream infile;
	DatabaseEngine dbEngine(&mem,&disk);

	if(argc>=2)
	{
		infile.open(argv[1]);
		while(getline(infile,query))
		{
			if(query == "QUIT" || query == "quit" || query == "q")
				break;
			dbEngine.execQuery(query);

		}
		infile.close();
	}
	else
	{
		while(getline(cin,query))
		{
			if(query == "QUIT" || query == "quit" || query == "q")
				break;
			dbEngine.execQuery(query);
		}
	}
	return 0;
}