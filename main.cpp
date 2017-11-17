#include "parser.h"
#include "helper.h"
using namespace std;

int main()
{
	Node *root;
	vector<string> field_names;
	vector<enum FIELD_TYPE>field_types;
	string query1 = "CREATE TABLE testTable ( q STR20, r INT, s STR20)";
	string query2 = "DELETE FROM course WHERE grade = \"E\"";
	string query3 = "SELECT DISTINCT col1,col2 FROM testTable1, table2 WHERE col1 = 9 AND col2 = 7 ORDER BY col1";
	string query4 = "INSERT INTO course (sid, grade, exam, project, homework) VALUES (3, \"E\", 100, 100, 100)";
	root = parseQuery(query1);
	//root->printTree(0);
	getAttributeTypeList(root,field_names,field_types);
	for(string &str:field_names)
		cout<<str<<endl;
	for(enum FIELD_TYPE t:field_types)
	{
		string str = (t==FIELD_TYPE::INT)?"INT":"STR20";
		cout<<str<<endl;
	}
	delete root;
	return 0;
}