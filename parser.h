#ifndef _PARSER_H
#define _PARSER_H

#include <vector>
#include <string>
#include <iostream>

using namespace std;

enum NODE_TYPE
{
	CREATE_QUERY,
	CREATE,
	TABLE,
	DROP_QUERY,
	DROP,
	WHERE,
	SEARCH_CONDITION,
	CONDITION_STR,
	ORDER,
	BY,
	DELETE_QUERY,
	DELETE,
	TABLE_NAME,
	ATTRIBUTE_TYPE_LIST,
	ATTRIBUTE_NAME,
	DATA_TYPE,
	INSERT_QUERY,
	INSERT,
	INTO,
	VALUES,
	ATTRIBUTE_LIST,
	INSERT_TUPLES,
	VALUE_LIST,
	VALUE,
	SELECT_QUERY,
	SELECT,
	DISTINCT,
	SELECT_LIST,
	SELECT_SUBLIST,
	STAR,
	COLUMN_NAME,
	FROM,
	TABLE_LIST
};

class Node
{
public:
	enum NODE_TYPE nodeType;
	string nodeVal;
	vector<Node*> children;
	Node(enum NODE_TYPE nodeType, string val)
	{
		this->nodeType = nodeType;
		this->nodeVal = val;
	}

	void printTree(int depth)
	{
		for(int i = 1;i<=depth;i++)
			cout<<"-";
		cout<<nodeVal<<endl;
		for(int i = 0;i<children.size();i++)
		{
			children[i]->printTree(depth+1);
		}
	}
	~Node()
	{
		for(int i = 0;i<children.size();i++)
			delete children[i];
	}
};

// Returns the root of the generated parse tree from the query
Node* parseQuery(string &query);
#endif