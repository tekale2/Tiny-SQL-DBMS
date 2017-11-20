#include <vector>
#include <string>
#include "databaseEngine.h"

using namespace std;

int main()
{
	MainMemory mem;
	Disk disk;
	DatabaseEngine dbEngine(&mem,&disk);
	string query1 = "CREATE TABLE testTable ( q STR20, r INT, s STR20)";
	string query2 = "DELETE FROM course WHERE grade = \"E\"";
	string query3 = "SELECT DISTINCT col1,col2 FROM testTable1, table2 WHERE col1 = 9 AND col2 = 7 ORDER BY col1";
	string query4 = "INSERT INTO course (sid, grade, exam, project, homework) VALUES (3, \"E\", 100, 100, 100)";
	string query5 = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
	dbEngine.execQuery(query1);
	dbEngine.execQuery(query5);
	return 0;
}