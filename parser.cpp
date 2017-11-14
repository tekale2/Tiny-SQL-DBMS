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