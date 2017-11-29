#include <iostream>
#include <stack>
#include <sstream>
#include "whereEval.h"

using namespace std;

/*----Global structs and variables----------*/
enum TOKEN_TYPE
{
    STR_CONST,
    INT_CONST
};

struct Tokens
{
    int intVal;
    string strVal;
};


/*--------------------Private Functions-------------*/

// tokenizes the where string
static vector<string> tokenizeWhere(string cond)
{
    string token;
    unordered_set<char> opList = {'+','-','*','=','>','<','(',')'};
    int start,i;
    vector<string> result;
    istringstream iss(cond);
    while(iss>>token)
    {
        start = 0;
        for(i = 0;i<token.length();i++)
        {
            if(opList.count(token[i]))
            {
                if(start<i)
                {
                    result.push_back(token.substr(start,i - start));
                }
                result.emplace_back(string(1,token[i]));
                start = i+1;
            }
        }
        if(start<i)
        {
            result.push_back(token.substr(start,i - start));
        }

    }
    return result;
}

static int precedence(string &str)
{
    int retVal =0;
    if(str == "*")
        retVal = 6;
    else if(str == "+" || str == "-")
        retVal = 5;
    else if(str == ">" || str == "<")
        retVal = 4;
    else if(str == "=")
        retVal = 3;
    else if(str == "AND")
        retVal = 2;
    else if(str == "OR")
        retVal = 1;
    else
        retVal = 0;
    return retVal;
}

static int evalStrings(string &s1,string &s2, string &op)
{
     int retVal =0;
    if(op == "=")
        retVal = (s1==s2);
    else
        retVal = 0;
    return retVal;   
}

static int evalInt(int v1, int v2, string &op)
{
        int retVal =0;
    if(op == "*")
        retVal = v1*v2;
    else if(op == "+")
        retVal = v1+v2;
    else if(op == "-")
        retVal = v1-v2;
    else if(op == ">")
        retVal = (v1>v2);
    else if(op == "<")
        retVal = (v1<v2);
    else if(op == "=")
        retVal = (v1==v2);
    else if(op == "AND")
        retVal = (v1 && v2);
    else if(op == "OR")
        retVal = (v1 || v2);
    else
        retVal = 0;
    return retVal;
}

/*--------------------Private Class Functions-------------*/

void CondEval::infixToPostfix(string cond)
{
    vector<string> tokens;
    stack<string> opStack;
    tokens = tokenizeWhere(cond);
    for(string &str:tokens)
    {
        if(str == "(")
        {
            opStack.push(str);
        }
        else if(str == ")")
        {
            while(!opStack.empty() && opStack.top() !="(")
            {
                postfix.push_back(opStack.top());
                opStack.pop();
            }
            if(!opStack.empty() && opStack.top() !="(")
                return;
            opStack.pop();
        }
        else if(ops.count(str))
        {
            while(!opStack.empty() && precedence(opStack.top())>= precedence(str))
            {
                postfix.push_back(opStack.top());
                opStack.pop();
            }
            opStack.push(str);
        }
        else
        {
            postfix.push_back(str);
        }
    }
    while(!opStack.empty())
    {
        postfix.push_back(opStack.top());
        opStack.pop();
    }  
    return;
}

/*--------------------public Class Functions-------------*/


CondEval::CondEval()
{
   ops = {"*","+","-",">","<","=","(",")","AND","OR"};
}

// Tokenizes the where condition and stores it as postfix string
void CondEval::intialize(string cond, unordered_map<string, enum FIELD_TYPE> colType)
{
	this->columnType = colType;
	postfix.clear();
	infixToPostfix(cond);
}

// Evaluates the tuple which is passed in as a map
bool CondEval::evaluate(unordered_map<string, Field> &colValues)
{
  stack<pair<Tokens,enum TOKEN_TYPE> > evalStack;
  pair<Tokens, enum TOKEN_TYPE> temp;
  int v1, v2, v3;
  string s1, s2;
  for(string &str:postfix)
  {
      if(ops.count(str))
      {
          temp = evalStack.top();
          evalStack.pop();
          if(temp.second == TOKEN_TYPE::STR_CONST)
          {
              s2 = temp.first.strVal;
              //cout<<"Token 2: "<<s2;
          }
          else
          {
              v2 = temp.first.intVal;
              //cout<<"Token 2: "<<v2;
          }
          
          temp = evalStack.top();
          evalStack.pop();
          if(temp.second == TOKEN_TYPE::STR_CONST)
          {
              s1 = temp.first.strVal;
              v3 = evalStrings(s1,s2,str);
              //cout<<"  Token 1: "<<s1;
          }
          else
          {
              v1 = temp.first.intVal;
              //cout<<"  Token 1: "<<v1;
              v3 = evalInt(v1,v2,str);
          }
          
            //cout<<" Operator: "<< str<< " Result: "<<v3<<endl;
            Tokens op;
            op.intVal = v3;
            temp = make_pair(op, INT_CONST);
            evalStack.push(temp);
            
        }
      
      else
      {
        if(colValues.count(str))
        {
        	//cout<<"found variable: "<<str<<endl;
        	Tokens op;
        	if(columnType[str] == FIELD_TYPE::INT)
        	{
	            op.intVal = colValues[str].integer;
	            temp = make_pair(op, INT_CONST);
        	}
        	else
        	{
        		op.strVal = *(colValues[str].str);
            	temp = make_pair(op, STR_CONST);
        	}
        }
        else
        {
            //cout<<str<<endl;
            Tokens op;
            try
            {
              //cout<<"found INT CONST: "<<str<<endl;
              op.intVal = stoi(str);
              temp = make_pair(op, INT_CONST);
            }
            catch(...)
            {
              //cout<<"found STR CONST: "<<str<<endl;
              op.strVal = str;
              temp = make_pair(op, STR_CONST);
            }    
            
        }
         evalStack.push(temp);
      }
  }
    bool result = evalStack.top().first.intVal;
    evalStack.pop();
    return result;
}
