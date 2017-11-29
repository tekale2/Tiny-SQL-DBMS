#ifndef _HELPER_H
#define _HELPER_H
#include "parser.h"
#include "./StorageManager/Field.h"
using namespace std;
// contains helper functions for the query execution

/* returns pointer to the required Node Type
 returns Null if not found*/
Node* getNode(enum NODE_TYPE nodeType, Node *root);

// returns true if there is a node of given nodeType
bool hasNode(enum NODE_TYPE nodeType, Node *root);

// searches and returns the value of first instance of specified NodeType
string getNodeVal(enum NODE_TYPE nodeType, Node *root);

// loads the vectors with dataTypes and columnsNames
void getAttributeTypeList(Node *root, vector<string>& field_names, vector<enum FIELD_TYPE>& field_types);

// Returns a vector of values for multiple instances of specified node type in the right order
vector<string> getNodeTypeLists(enum NODE_TYPE nodeType, Node *root);

// gets and returns the column name of the orderby column
string getOrdeByColumnName(Node *root);

#endif