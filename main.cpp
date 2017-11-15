#include <iostream>
#include <string>

#include "parser.h"

using namespace std;

int main()
{
	Node *root;
	string query = "CREATE TABLE testTable ( q STR20, r INT, s STR20)";
	root = parseQuery(query);
	root->printTree(0);
	delete root;
	return 0;
}