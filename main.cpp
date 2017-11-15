#include "parser.h"

using namespace std;

int main()
{
	Node *root;
	string query = "CREATE TABLE testTable ( q STR20, r INT, s STR20)";
	string query2 = "DELETE FROM course WHERE grade = \"E\"";
	string query3 = "SELECT DISTINCT col1,col2 FROM testTable1, table2 WHERE col1 = 9 AND col2 = 7 ORDER BY col1";
	root = parseQuery(query3);
	root->printTree(0);
	delete root;
	return 0;
}