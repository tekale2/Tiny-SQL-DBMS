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
				cout<<query<<endl;
				if(query == "STOP")
					break;
				cout<<dbEngine.execQuery(query)<<endl;

		}
		infile.close();
	}
	else
	{
		while(getline(cin,query))
		{
				if(query == "STOP")
					break;
				cout<<dbEngine.execQuery(query)<<endl;
		}
	}
	return 0;
}