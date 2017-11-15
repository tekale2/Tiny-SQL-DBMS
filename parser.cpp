#include <algorithm>
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
static Node* genCreateTree(vector<string> &tokens)
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

// Generates a parse tree for drop table statement
static Node* genDropTableTree(vector<string> &tokens)
{
	Node *root = new Node(NODE_TYPE::DROP_QUERY,"<drop-query>");
	root->children.push_back(new Node(NODE_TYPE::DROP,"DROP"));
	root->children.push_back(new Node(NODE_TYPE::TABLE,"TABLE"));
	root->children.push_back(new Node(NODE_TYPE::TABLE_NAME,tokens[2]));

	return root;
}

// Generates a parse tree for delete statement
static Node* genDeleteTree(vector<string> &tokens)
{
	string condStr;
	int idx;
	Node *root = new Node(NODE_TYPE::DELETE_QUERY,"<delete-query>");
	root->children.push_back(new Node(NODE_TYPE::DELETE,"DELETE"));
	root->children.push_back(new Node(NODE_TYPE::FROM,"FROM"));
	root->children.push_back(new Node(NODE_TYPE::TABLE_NAME,tokens[2]));
	if(tokens.size() > 3)
	{
		root->children.push_back(new Node(NODE_TYPE::WHERE,"WHERE"));
		idx = 4;
		condStr = tokens[idx];
		for(idx = 5;idx<tokens.size();idx++)
		{
			condStr+=" ";
			condStr+=tokens[idx];
		}
		Node *temp = new Node(NODE_TYPE::SEARCH_CONDITION,"<search-condition>");
		temp->children.push_back(new Node(NODE_TYPE::CONDITION_STR,condStr));
		root->children.push_back(temp);
	}
	return root;
}

// Generates a parse tree for select statement
static Node* genSelectTree(vector<string> &tokens)
{
	Node *temp, *curr;
	string condStr;
	int idx = 1;
	int endIdx;
	Node *root = new Node(NODE_TYPE::SELECT_QUERY, "<select-query>");
	root->children.push_back(new Node(NODE_TYPE::SELECT,"SELECT"));
	if(tokens[1] == "DISTINCT")
	{
		root->children.push_back(new Node(NODE_TYPE::DISTINCT,"DISTINCT"));
		idx++;
	}
	temp = new Node(NODE_TYPE::SELECT_LIST,"<select-list>");
	root->children.push_back(temp);
	// get select list
	if(tokens[idx] == "*")
	{
		temp->children.push_back(new Node(NODE_TYPE::STAR,"*"));
		idx++;
	}
	else
	{
		auto it1 = find(tokens.begin(),tokens.end(),"FROM");
		endIdx = it1 - tokens.begin();
		curr = temp;
		while(idx < endIdx)
		{
			temp = new Node(NODE_TYPE::SELECT_SUBLIST,"<select-sublist>");
			curr->children.push_back(temp);
			curr = temp;
			curr->children.push_back(new Node(NODE_TYPE::COLUMN_NAME,tokens[idx]));
			idx+=2;
		}
		idx = endIdx;
	}
	root->children.push_back(new Node(NODE_TYPE::FROM,"FROM"));

	// get table list
	auto it = find(tokens.begin(),tokens.end(),"WHERE");
	if(it == tokens.end())
	{
		it = find(tokens.begin(),tokens.end(),"ORDER");
		if(it==tokens.end())
			endIdx = tokens.size();
		else
			endIdx = it - tokens.begin();

	}
	else
		endIdx = it - tokens.begin();

	curr = root;
	idx++;
	while(idx < endIdx)
	{
		temp = new Node(NODE_TYPE::TABLE_LIST,"<table-list>");
		curr->children.push_back(temp);
		curr = temp;
		curr->children.push_back(new Node(NODE_TYPE::TABLE_NAME,tokens[idx]));
		idx+=2;
	}


	// where cond
	it = find(tokens.begin(),tokens.end(),"WHERE");
	if(it!=tokens.end())
	{
		root->children.push_back(new Node(NODE_TYPE::WHERE,"WHERE"));
		idx =  it - tokens.begin();
		idx++;
		condStr = tokens[idx++];
		it = find(tokens.begin(),tokens.end(),"ORDER");
		endIdx = (it==tokens.end())? tokens.size():(it-tokens.begin());
		for(;idx<endIdx;idx++)
		{
			condStr+=" ";
			condStr+=tokens[idx];
		}
		Node *temp = new Node(NODE_TYPE::SEARCH_CONDITION,"<search-condition>");
		temp->children.push_back(new Node(NODE_TYPE::CONDITION_STR,condStr));
		root->children.push_back(temp);
	}
	it = find(tokens.begin(),tokens.end(),"ORDER");
	if(it!=tokens.end())
	{
		idx = it - tokens.begin();
		root->children.push_back(new Node(NODE_TYPE::ORDER,"ORDER"));
		root->children.push_back(new Node(NODE_TYPE::BY,"BY"));
		root->children.push_back(new Node(NODE_TYPE::COLUMN_NAME,tokens[idx+2]));
	}

	return root;
}

// Generates a parse tree for Insert statement
static Node* genInsertTree(vector<string> &tokens)
{
	int idx = 3;
	int endIdx;
	vector<string> selectTokens;
	Node *curr, *temp, *insertTuples;
	Node *root = new Node(NODE_TYPE::INSERT_QUERY, "<insert-query>");
	root->children.push_back(new Node(NODE_TYPE::INSERT,"INSERT"));
	root->children.push_back(new Node(NODE_TYPE::INTO,"INTO"));
	root->children.push_back(new Node(NODE_TYPE::TABLE_NAME,tokens[2]));

	//get attribute list
	auto it = find(tokens.begin(),tokens.end(),")");
	endIdx = it - tokens.begin();

	curr = root;
	idx++;
	while(idx < endIdx)
	{
		temp = new Node(NODE_TYPE::ATTRIBUTE_LIST,"<attribute-list>");
		curr->children.push_back(temp);
		curr = temp;
		curr->children.push_back(new Node(NODE_TYPE::ATTRIBUTE_NAME,tokens[idx]));
		idx+=2;
	}
	insertTuples = new Node(NODE_TYPE::INSERT_TUPLES,"<insert-tuples>");
	root->children.push_back(insertTuples);
	if(tokens[endIdx+1] != "VALUES")
	{
		for(int i = endIdx+1;i<tokens.size();i++)
			selectTokens.push_back(tokens[i]);
		insertTuples->children.push_back(genSelectTree(selectTokens));
	}
	else
	{
		insertTuples->children.push_back(new Node(NODE_TYPE::VALUES,"VALUES"));
		curr = insertTuples;
		for(idx = endIdx+3;idx<tokens.size();idx+=2)
		{
			temp = new Node(NODE_TYPE::VALUE_LIST,"<value-list>");
			curr->children.push_back(temp);
			curr = temp;
			curr->children.push_back(new Node(NODE_TYPE::VALUE,tokens[idx]));
		}
	}
	return root;
}

// Generates a parse tree for the input query and returns the root
Node* parseQuery(string &query)
{
	Node *root;
	vector<string> tokens;
	tokens = tokenize(query);
	if(tokens.size()<3)
		return NULL;
	// generate tree based on type of the statement
	if(tokens[0] == "CREATE" && tokens[1] == "TABLE")
		root = genCreateTree(tokens);
	else if(tokens[0] == "DROP" && tokens[1] == "TABLE" && tokens.size()==3)
		root = genDropTableTree(tokens);
	else if(tokens[0] == "DELETE" && tokens[1] == "FROM")
		root = genDeleteTree(tokens);
	else if(tokens[0] == "INSERT" && tokens[1] == "INTO")
		root = genInsertTree(tokens);
	else if(tokens[0] == "SELECT")
		root = genSelectTree(tokens);
	else
		root = NULL;
	return root;
}