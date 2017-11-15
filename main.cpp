#include <iostream>
#include <string>

#include "parser.h"

using namespace std;

int main()
{
	Node *root;
	string query = "CREATE TABLE testTable ( q STR20, r INT, s STR20)";
	string query2 = "DROP TABLE testTable";
	root = parseQuery(query2);
	root->printTree(0);
	delete root;
	return 0;
}