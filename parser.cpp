#include "parser.h"

using namespace std;
// helper function to tokenize the the input query into tokens
vector<string> tokenize(string &query)
{
	vector<string> result;
	int firstIdx = 0;
	int i = 0;
	bool inQuotes = false;
	for(i = 0; i<query.size();i++)
	{
		if(!inQuotes && query[i] == '"')
	    {
			inQuotes = true;
			// allow for space
			if(firstIdx <i-1)
				result.push_back(query.substr(firstIdx,i - firstIdx));
			firstIdx = i+1;
		}
		else if(inQuotes && query[i] == '"')
		{
			inQuotes = false;
			if(firstIdx <i)
				result.push_back(query.substr(firstIdx,i - firstIdx));
			firstIdx = i+1;

		}
		else if(!inQuotes)
		{
			switch(query[i])
			{
				case ' ':
					if(firstIdx <i)
						result.push_back(query.substr(firstIdx,i - firstIdx));
					firstIdx = i+1;
				break;
				case ',':
				case '(':
				case ')':
					if(firstIdx <i)
						result.push_back(query.substr(firstIdx,i - firstIdx));
					result.emplace_back(string(1,query[i]));
					firstIdx = i+1;
				break;
				default:
				break;
			}
	    }
	}

	if(firstIdx <i)
		result.push_back(query.substr(firstIdx,i - firstIdx));

	return result;
}

// Generates a parse tree for create statement
Node *genCreateTree(vector<string> &tokens)
{
	Node *curr;
	int idx;
	Node *root = new Node(NODE_TYPE::CREATE_QUERY,"<create_query>");
	root->children.push_back(new Node(NODE_TYPE::CREATE,"CREATE"));
	root->children.push_back(new Node(NODE_TYPE::TABLE,"TABLE"));
	root->children.push_back(new Node(NODE_TYPE::TABLE_NAME,tokens[2]));
	idx = 4;
	curr = root;
	while(tokens[idx+2] == ","||tokens[idx+2] == ")")
	{
		Node *temp = new Node(NODE_TYPE::ATTRIBUTE_TYPE_LIST,"<attribute-type-list>");
		curr->children.push_back(temp);
		curr = temp;
		curr->children.push_back(new Node(NODE_TYPE::ATTRIBUTE_NAME,tokens[idx]));
		curr->children.push_back(new Node(NODE_TYPE::DATA_TYPE,tokens[idx+1]));
		if(tokens[idx+2] == ")")
			break;
		idx+=3;
	}

	return root;
}

// Generates a parse tree and returns the root
Node *parseQuery(string &query)
{
	Node *root;
	vector<string> tokens;
	tokens = tokenize(query);
	if(tokens.size()<3)
		return NULL;
	// generate tree based on type of the statement
	if(tokens[0] == "CREATE" && tokens[1] == "TABLE")
		root = genCreateTree(tokens);
	else
		root = NULL;
	return root;
}