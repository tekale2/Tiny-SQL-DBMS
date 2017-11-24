#include "helper.h"
#include <queue>
using namespace std;

/* returns pointer to the required Node Type
 returns Null if not found*/
Node* getNode(enum NODE_TYPE nodeType, Node *root)
{
	queue<Node*> nodeQ;
	Node *temp;
	Node *found = NULL;
	nodeQ.push(root);
	while(!nodeQ.empty())
	{
		temp = nodeQ.front();
		nodeQ.pop();
		if(temp->nodeType == nodeType)
		{
			found = temp;
			break;
		}
		for(int i = 0;i<temp->children.size();i++)
			nodeQ.push(temp->children[i]);
	}
	return found;
}

// returns true if there is a node of given nodeType
bool hasNode(enum NODE_TYPE nodeType, Node *root)
{
	queue<Node*> nodeQ;
	Node *temp;
	bool found = false;
	nodeQ.push(root);
	while(!nodeQ.empty())
	{
		temp = nodeQ.front();
		nodeQ.pop();
		if(temp->nodeType == nodeType)
		{
			found = true;
			break;
		}
		for(int i = 0;i<temp->children.size();i++)
			nodeQ.push(temp->children[i]);
	}
	return found;
}

// searches and returns the value of first instance of specified NodeType
string getNodeVal(enum NODE_TYPE nodeType, Node *root)
{
	queue<Node*> nodeQ;
	string result;
	Node *temp;
	Node *found = NULL;
	nodeQ.push(root);
	while(!nodeQ.empty())
	{
		temp = nodeQ.front();
		nodeQ.pop();
		if(temp->nodeType == nodeType)
		{
			found = temp;
			break;
		}
		for(int i = 0;i<temp->children.size();i++)
			nodeQ.push(temp->children[i]);
	}
	result = (found==NULL)? "Not Found":found->nodeVal;
	return result;	
}

// loads the vectors with dataTypes and columnsNames
void getAttributeTypeList(Node *root, vector<string>& field_names, vector<enum FIELD_TYPE>& field_types)
{
	Node *attrRoot = getNode(NODE_TYPE::ATTRIBUTE_TYPE_LIST,root);
	Node *curr;
	enum FIELD_TYPE f;
	curr = attrRoot;
	if(curr==NULL)
		return;
	while(curr->children.size()>=2)
	{
		field_names.push_back(curr->children[0]->nodeVal);
		f = (curr->children[1]->nodeVal == "INT")? (FIELD_TYPE::INT): (FIELD_TYPE::STR20);
		field_types.push_back(f);
		if(curr->children.size() == 2)
			break;
		else
			curr = curr->children[2];

	}
	return;
}

// Returns a vector of values for multiple instances of specified node type in the right order
vector<string> getNodeTypeLists(enum NODE_TYPE nodeType, Node *root)
{
	vector<string> result;
	Node *curr = getNode(nodeType,root);
	if(curr==NULL)
		return result;
	while(curr->children.size()>=1)
	{
		result.push_back(curr->children[0]->nodeVal);
		if(curr->children.size() == 1)
			break;
		else
			curr = curr->children[1];
	}
	return result;
}