#include <iostream>

#include "parser.h"

using namespace std;

int main()
{
	vector<string> result;
	string query = "INSERT INTO testTable ( Id , kl, e) VALUES (\"30\", \"text\", \"34\" )";
	result = tokenize(query);
	for(string &str:result)
		cout<<str<<endl;
	return 0;
}