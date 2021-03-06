#ifndef _WHERE_EVAL_H
#define _WHERE_EVAL_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "./StorageManager/Field.h"

using namespace std;

class CondEval
{
private:
  vector<string> postfix;
  unordered_set<string> ops;
  unordered_map<string, enum FIELD_TYPE> columnType;
  void infixToPostfix(string cond);
public:
  CondEval();
  void intialize(string cond, unordered_map<string, enum FIELD_TYPE> colType);
  bool evaluate(unordered_map<string, Field> &colValues);
};

vector<string> getVarsInwhere(string &whereString, unordered_set<string> &allColumnSet);
void splitByAndPopulate(string &whereString, unordered_set<string> &allColumnSet,\
	unordered_map<string,unordered_set<string>> &whereSplit);
string getWhere(unordered_map<string,unordered_set<string>> &whereSplit, \
	unordered_set<string> &currJoinedTables);
#endif