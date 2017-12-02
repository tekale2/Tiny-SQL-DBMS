#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include "databaseEngine.h"

using namespace std;

/*--------------Private Functions-------------------*/

// Compares the fields of 2 schemas
static bool cmpFields(Schema s1, Schema s2)
{
	vector<enum FIELD_TYPE> f1 = s1.getFieldTypes();
	vector<enum FIELD_TYPE> f2 = s2.getFieldTypes();
	return f1 == f2;
}

// Converts a given schema to a map
static void schemaToMap(unordered_map<string,enum FIELD_TYPE> &colType, Schema &schema)
{
	vector<string>  field_names;
	vector<enum FIELD_TYPE> field_types;

	field_names = schema.getFieldNames();
	field_types = schema.getFieldTypes();
	for(int i = 0;i<field_types.size();i++)
		colType[field_names[i]] = field_types[i];
}

// Converts a given tuple to a column map
static void tupleToMap(Schema &schema, Tuple &tuple, unordered_map<string, Field> &colValues)
{
	vector<string>  field_names;
	field_names = schema.getFieldNames();
	for(int i = 0;i<field_names.size();i++)
	{
		colValues[field_names[i]] = tuple.getField(field_names[i]);
	}
	return;
}

// Join 2 schemas
static Schema joinSchemas(Schema &sch1, Schema &sch2, Relation *rel1, Relation *rel2)
{
	string rel1Name, rel2Name;
	vector<string> field_names_1, field_names_2, field_names_out;
	vector<enum FIELD_TYPE> field_types_1, field_types_2, field_types_out;
	field_names_1 = sch1.getFieldNames();
	field_names_2 = sch2.getFieldNames();
	field_types_1 = sch1.getFieldTypes();
	field_types_2 = sch2.getFieldTypes();
	rel1Name = rel1->getRelationName();
	rel2Name = rel2->getRelationName();
	for(int i = 0;i<field_names_1.size();i++)
	{
		if(field_names_1[i].find(".")==string::npos)
		{
			field_names_1[i] = rel1Name + "."+field_names_1[i];
		}
	}
	
	for(int i = 0;i<field_names_2.size();i++)
	{
		if(field_names_2[i].find(".")==string::npos)
		{
			field_names_2[i] = rel2Name + "."+field_names_2[i];
		}
	}
	field_names_out.insert(field_names_out.end(),field_names_1.begin(),field_names_1.end());
	field_names_out.insert(field_names_out.end(),field_names_2.begin(),field_names_2.end());
	field_types_out.insert(field_types_out.end(),field_types_1.begin(),field_types_1.end());
	field_types_out.insert(field_types_out.end(),field_types_2.begin(),field_types_2.end());
	Schema outputJoined(field_names_out,field_types_out);

	return outputJoined;
}

// Join 2 tuples

static Tuple joinTuples(Tuple &t1,Tuple &t2, Relation *outRel)
{
	vector<enum FIELD_TYPE> field_types;
	int i,k;
	i = 0;
	k = 0;
	Schema schema = outRel->getSchema();
	field_types = schema.getFieldTypes();

	Tuple outTuple = outRel->createTuple();
	for(i = 0; i<t1.getNumOfFields();i++)
	{
		if(field_types[i] == FIELD_TYPE::INT)
			outTuple.setField(i, t1.getField(i).integer);
		else
			outTuple.setField(i, *(t1.getField(i).str));

	}
	while(k<t2.getNumOfFields())
	{
		if(field_types[i] == FIELD_TYPE::INT)
			outTuple.setField(i, t2.getField(k).integer);
		else
			outTuple.setField(i, *(t2.getField(k).str));
		i++;
		k++;
	}
	return outTuple;
}

//Project Tuple

static Tuple projectTuple(Tuple &t1, Relation *rel)
{
	vector<enum FIELD_TYPE> field_types;
	vector<string> field_names;
	Schema schema = rel->getSchema();
	field_types = schema.getFieldTypes();
	field_names = schema.getFieldNames();

	Tuple outTuple = rel->createTuple();
	for(int j = 0;j<field_names.size();j++)
	{
		if(field_types[j] == FIELD_TYPE::INT)
		{
			int intVal = t1.getField(field_names[j]).integer;
			outTuple.setField(field_names[j],intVal);
		}
		else
		{
			string strVal = *(t1.getField(field_names[j]).str);
			outTuple.setField(field_names[j],strVal);
		}
	}

	return outTuple;
}
//Projected Schema
static Schema getOutputSchema(Schema &joinedSchema, unordered_set<string> &projectionSet)
{
	vector<string> field_names_in, field_names_out;
	vector<enum FIELD_TYPE> field_types_in, field_types_out;
	field_names_in = joinedSchema.getFieldNames();
	field_types_in = joinedSchema.getFieldTypes();

	for(int i = 0;i<field_names_in.size();i++)
	{
		if(projectionSet.count(field_names_in[i]))
		{
			field_names_out.push_back(field_names_in[i]);
			field_types_out.push_back(field_types_in[i]);
		}
	}

	Schema projected(field_names_out, field_types_out);

	return projected;
}

/*--------------Private Class Functions-------------*/

// Main physical create query executor
bool DatabaseEngine::execCreateQuery(Node *root)
{
	string table_name;
	vector<string> field_names;
  	vector<enum FIELD_TYPE> field_types;
	Relation* relation_ptr;
	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	getAttributeTypeList(root, field_names, field_types);

	// create a new schema
	Schema schema(field_names,field_types);
	relation_ptr = schema_manager.createRelation(table_name,schema);
	if(relation_ptr == NULL)
		return false;
	return true;
}

// Main physical drop query executor
bool DatabaseEngine::execDropQuery(Node *root)
{
	string table_name;
	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	schema_manager.deleteRelation(table_name);
	return true;
}

// Main physical insert query executor
bool DatabaseEngine::execInsertQuery(Node *root)
{
	string table_name;
	vector<string> values, attrList;
	vector<enum FIELD_TYPE> field_types;
	vector<Tuple> output, tuples;
	Block *memBlockPtr;
	int freeMemIdx, numBlocks;
	int memIdx,value;
	bool validTuple = true;
	enum FIELD_TYPE field_type;
	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	Relation* relation_ptr;
	Relation* outputRel;
	Node* selectRoot;
	relation_ptr = schema_manager.getRelation(table_name);

	if(relation_ptr == NULL)
		return false;

	Schema schema = relation_ptr->getSchema();
	if( hasNode(NODE_TYPE::VALUES, root))
	{
		values = getNodeTypeLists(NODE_TYPE::VALUE_LIST,root);
		attrList = getNodeTypeLists(NODE_TYPE::ATTRIBUTE_LIST,root);
		Tuple tuple =  relation_ptr->createTuple();

		// make tuple
		for(int i = 0; i<values.size();i++)
		{
			field_type = schema.getFieldType(attrList[i]);
			if(field_type == FIELD_TYPE::INT)
			{
				try
				{
					value = stoi(values[i]);
				}
				catch(...)
				{
					if(values[i] == "NULL")
						value =0;
					else
					{
						log("Exception occurred when coverting string to int");
						return false;
					}
				}
				validTuple = tuple.setField(attrList[i],value);
			}
			else
			{
				validTuple = tuple.setField(attrList[i],values[i]);
			}
			if(!validTuple)
				return false;
		}

		//get a free mem block and append
		memIdx = buffer_manager.getFreeBlockIdx();
		if(memIdx == -1)
		{
			log("Out of free memory blocks");
			return false;
		}

		appendTupleToRelation(relation_ptr, mem, memIdx, tuple);
		buffer_manager.storeFreeBlockIdx(memIdx);
	}
	else
	{
		selectRoot = getNode(NODE_TYPE::SELECT_QUERY, root);
		if(selectRoot==NULL)
			return false;
		outputRel = execSelectQuery(root, false);
		if(outputRel ==NULL)
			return false;
		
		Schema outputSchema = outputRel->getSchema();
		if(!cmpFields(outputSchema,schema))
			return false;

		field_types = outputSchema.getFieldTypes();
		// get tuple from output and insert into vector
		if(resultInMemory)
		{
			for(int i:memoryBlockIndices)
			{
				memBlockPtr = mem->getBlock(i);
				tuples = memBlockPtr->getTuples();
				for(Tuple &t: tuples)
				{
					Tuple outTuple = relation_ptr->createTuple();
					for(int j = 0; j<field_types.size();j++)
					{
						if(field_types[j] == FIELD_TYPE::INT)
						{
							int intVal = t.getField(j).integer;
							outTuple.setField(j, intVal);
						}
						else
						{
							string strVal = *(t.getField(j).str);
							outTuple.setField(j, strVal);
						}
					}

					output.push_back(outTuple);	
				}
			}
		}
		else
		{
			numBlocks = outputRel->getNumOfBlocks();
			freeMemIdx = buffer_manager.getFreeBlockIdx();

			for(int i= 0; i<numBlocks;i++)
			{
				memBlockPtr = mem->getBlock(freeMemIdx);
				memBlockPtr->clear();
				//load the memInBlock
				outputRel->getBlock(i,freeMemIdx);
				tuples = memBlockPtr->getTuples();
				for(Tuple &t: tuples)
				{
					Tuple outTuple = relation_ptr->createTuple();
					for(int j = 0; j<field_types.size();j++)
					{
						if(field_types[j] == FIELD_TYPE::INT)
						{
							int intVal = t.getField(j).integer;
							outTuple.setField(j, intVal);
						}
						else
						{
							string strVal = *(t.getField(j).str);
							outTuple.setField(j, strVal);
						}
					}

					output.push_back(outTuple);	
				}

			}
			buffer_manager.storeFreeBlockIdx(freeMemIdx);
		}
		cleanUp();

		//get a free mem block and append
		memIdx = buffer_manager.getFreeBlockIdx();
		if(memIdx == -1)
		{
			log("Out of free memory blocks");
			return false;
		}

		for(Tuple &tuple:output)
			appendTupleToRelation(relation_ptr, mem, memIdx, tuple);
		
		buffer_manager.storeFreeBlockIdx(memIdx);

	}

	return true;
}

// Main physical delete query executor
bool DatabaseEngine::execDeleteQuery(Node *root)
{
	unordered_map<string,enum FIELD_TYPE> colType;
	unordered_map<string, Field> colValues;
	string table_name, whereString;
	CondEval whereCondEval;
	vector<Tuple> tuples;
	int memInIdx, memOutIdx, numBlocks, writeIdx;
	Block *in;
	Block *out;
	Relation* relation_ptr;
	

	table_name = getNodeVal(NODE_TYPE::TABLE_NAME,root);
	relation_ptr = schema_manager.getRelation(table_name);
	if(relation_ptr == NULL)
		return false;

	if( !hasNode(NODE_TYPE::WHERE, root))
	{
		relation_ptr->deleteBlocks(0);
      	return true;
	}
	//intilize whereCond
	Schema schema = relation_ptr->getSchema();
	schemaToMap(colType, schema);

	whereString =  getNodeVal(NODE_TYPE::CONDITION_STR,root);
	whereCondEval.intialize(whereString,colType);

	// get 2 memory buffers
	memInIdx = buffer_manager.getFreeBlockIdx();
	memOutIdx = buffer_manager.getFreeBlockIdx();
	out = mem->getBlock(memOutIdx);
	numBlocks = relation_ptr->getNumOfBlocks();

	
	writeIdx = 0;

	for(int i = 0; i<numBlocks;i++)
	{
		// load block into 1 buffer
		relation_ptr->getBlock(i,memInIdx);
		in = mem->getBlock(memInIdx);
		tuples = in->getTuples();
		for(Tuple &t:tuples)
		{
			tupleToMap(schema, t, colValues);
			if(whereCondEval.evaluate(colValues))
				continue;
		    if (out->isFull()) 
		    {
				relation_ptr->setBlock(writeIdx,memOutIdx); //write back to the relation
				out->clear(); //clear the block
				out->appendTuple(t); // append the tuple
				writeIdx++;
		    } else {
		    	out->appendTuple(t); // append the tuple
		    }
		}
	}
	if(!(out->isEmpty()))
	{
		relation_ptr->setBlock(writeIdx,memOutIdx); //write back to the relation
		out->clear(); //clear the block
		writeIdx++;
	}
	relation_ptr->deleteBlocks(writeIdx);

	buffer_manager.storeFreeBlockIdx(memOutIdx);
	buffer_manager.storeFreeBlockIdx(memInIdx);

	return true;
}

// Exectues select statement for single table. Output is stored in memory if small
// Or else the output is stored as temp relation on the disk

Relation* DatabaseEngine::tableScan(string &tableName, vector<string> &selectList, string whereCond)
{
	unordered_map<string,enum FIELD_TYPE> colType;
	unordered_map<string, Field> colValues;
	CondEval whereCondEval;
	bool whereExists, selectTuple;
	
	Schema schema;
	Relation *relation_ptr, *outRelation_ptr;
	string tempRelName;
	vector<string> field_names;
	vector<string> out_field_names;
	vector<enum FIELD_TYPE> field_types;
	vector<enum FIELD_TYPE> out_field_types;

	Block *input, *output;
	int inMemIdx, currIdx, numBlocks;
	vector<Tuple> inputTuples;

	relation_ptr = schema_manager.getRelation(tableName);
	if(relation_ptr == NULL)
		return NULL;
	schema = relation_ptr->getSchema();
	field_types = schema.getFieldTypes();
	field_names = schema.getFieldNames();
	
	if(selectList[0] == "*")
	{
		for(int i = 0; i<field_names.size();i++)
		{
			out_field_types.push_back(field_types[i]);
			out_field_names.push_back(field_names[i]);
		}
	}
	else
	{
		// remove "." from the column names
		for(int i = 0; i<selectList.size();i++)
		{
			size_t pos = selectList[i].find(".");
			if(pos != string::npos)
				selectList[i] = selectList[i].substr(pos+1);
		}

		for(int i = 0; i<field_names.size();i++)
		{
			if(count(selectList.begin(),selectList.end(),field_names[i]))
			{
				out_field_names.push_back(field_names[i]);
				out_field_types.push_back(field_types[i]);
			}
		}

	}

	Schema outputSchema(out_field_names,out_field_types);


	tempRelName = "temp_table#1";
	outRelation_ptr = schema_manager.createRelation(tempRelName,outputSchema);
	if(outRelation_ptr == NULL)
		return NULL;

	resultInMemory = true;

	if(whereCond.length()>0)
		whereExists = true;
	else
		whereExists = false;

	// initialize where condition evaluator
	if(whereExists)
	{
		schemaToMap(colType, schema);
		whereCondEval.intialize(whereCond,colType);
	}

	numBlocks = relation_ptr->getNumOfBlocks();
	if(numBlocks ==0)
		resultInMemory = false;
	inMemIdx = buffer_manager.getFreeBlockIdx();
	input = mem->getBlock(inMemIdx);

	currIdx = buffer_manager.getFreeBlockIdx();
	memoryBlockIndices.push_back(currIdx);

	for(int i = 0; i<numBlocks;i++)
	{
		vector<Tuple> outputTuples;
		// Step 1 extract and project the tuples
		relation_ptr->getBlock(i,inMemIdx);
		inputTuples = input->getTuples();
		for(Tuple &t:inputTuples)
		{
			if(whereExists)
			{
				tupleToMap(schema, t, colValues);
				if(!(whereCondEval.evaluate(colValues)))
					selectTuple = false;
				else
					selectTuple = true;

			}
			else
				selectTuple = true;
			if(selectTuple)
			{
				Tuple outTuple = outRelation_ptr->createTuple();
				for(int j = 0; j<out_field_names.size();j++)
				{
					if(out_field_types[j] == FIELD_TYPE::INT)
					{
						int intVal = t.getField(out_field_names[j]).integer;
						outTuple.setField(out_field_names[j], intVal);
					}
					else
					{
						string strVal = *(t.getField(out_field_names[j]).str);
						outTuple.setField(out_field_names[j], strVal);
					}
				}

				outputTuples.push_back(outTuple);
			}
		}

		// Step 2 insert the extracted/projected tuples into mem/relation
		for(Tuple &t:outputTuples)
		{
			if(!resultInMemory)
			{
				appendTupleToRelation(outRelation_ptr, mem, currIdx, t);
			}
			else
			{
				if(!appendTupleToMemory(mem, currIdx, t))
				{
					int temp = buffer_manager.getFreeBlockIdx();
					if(temp==-1)
					{
						// flush all blocks to disk
						outRelation_ptr->setBlocks(0,memoryBlockIndices[0],memoryBlockIndices.size());
						// release memory
						buffer_manager.releaseBulkIdx(memoryBlockIndices);
						memoryBlockIndices.clear();
						// get 1 block and push back
						currIdx = buffer_manager.getFreeBlockIdx();
						memoryBlockIndices.push_back(currIdx);
						appendTupleToRelation(outRelation_ptr, mem, currIdx, t);
						resultInMemory = false;
					}
					else
					{
						currIdx = temp;
						memoryBlockIndices.push_back(currIdx);
						appendTupleToMemory(mem, currIdx, t);
					}
				}
			}
		}

	}
	// release input memory and sort the indices
	buffer_manager.storeFreeBlockIdx(inMemIdx);
	if(!resultInMemory)
	{
		buffer_manager.storeFreeBlockIdx(memoryBlockIndices.back());
		memoryBlockIndices.pop_back();
		buffer_manager.sort();
	}
	return outRelation_ptr;
}

Relation* DatabaseEngine::execSelectQuery(Node *root, bool doLog)
{
	vector<string> selectList;
	vector<string> tableList;
	string whereString;
	string orderByColumn;
	bool hasDistinct, hasOrderBy;
	Relation *finalTempRel;
	

	hasDistinct = hasNode(NODE_TYPE::DISTINCT,root);
	hasOrderBy  = hasNode(NODE_TYPE::ORDER,root);
	if(hasOrderBy)
		orderByColumn = getOrderByColumnName(root);
	
	// get select list
	if(hasNode(NODE_TYPE::STAR,root))
		selectList.push_back("*");
	else
		selectList = getNodeTypeLists(NODE_TYPE::SELECT_SUBLIST, root);
		
	
	// get table list
	tableList = getNodeTypeLists(NODE_TYPE::TABLE_LIST, root);
	
	// get where condition

	if(hasNode(NODE_TYPE::WHERE, root))
		whereString =  getNodeVal(NODE_TYPE::CONDITION_STR,root);

	if(tableList.size()== 1)
	{
		if(hasOrderBy)
		{
			size_t pos = orderByColumn.find(".");
			if(pos != string::npos)
				orderByColumn = orderByColumn.substr(pos+1);

			if(count(selectList.begin(),selectList.end(),orderByColumn) == 0)
				selectList.push_back(orderByColumn);
		}

		finalTempRel = tableScan(tableList[0],selectList,whereString);
		if(finalTempRel == NULL)
		{
			cleanUp();
			return NULL;
		}

		tempRelations.push_back(finalTempRel);
	}
	else
	{
		// TODO: implemenet multitbale select
		if(count(selectList.begin(),selectList.end(),orderByColumn) == 0)
			selectList.push_back(orderByColumn);
		finalTempRel = lqpOptimize(tableList,selectList,whereString);
		if(finalTempRel == NULL)
		{
			cleanUp();
			return NULL;
		}
	}

	if(hasOrderBy && hasDistinct)
		finalTempRel = execDistinct(finalTempRel,orderByColumn);
	else if(hasOrderBy)
		finalTempRel = execOrderBy(finalTempRel,orderByColumn);
	else if(hasDistinct)
	{
		Schema sch= finalTempRel->getSchema();
		finalTempRel = execDistinct(finalTempRel,sch.getFieldNames()[0]);
	}

	if(doLog)
		log(finalTempRel);
	
	return finalTempRel;

}

// Performs logical query optimizations for joins on Select query
Relation* DatabaseEngine::lqpOptimize(vector<string> &tableList, vector<string> &selectList,string &whereString)
{
	unordered_set<string> projectionSet(selectList.begin(),selectList.end());
	Relation *outputRel, *rel1, *rel2;
	int num1Blocks, num2Blocks;
	if(tableList.size() == 2)
	{
		rel1 = schema_manager.getRelation(tableList[0]);
		rel2 = schema_manager.getRelation(tableList[1]);
		num1Blocks = rel1->getNumOfBlocks();
		num2Blocks = rel2->getNumOfBlocks();
		if(num2Blocks < num1Blocks)
			swap(rel1,rel2);
		outputRel = crossJoinRelations(rel1, rel2, projectionSet, whereString);
	}
	else
	{
		return NULL;
	}

	return outputRel;
}

// Cross Join 2 tables
Relation* DatabaseEngine::crossJoinRelations(Relation *rel1, Relation* rel2,\
	unordered_set<string> &projectionSet, string &whereString)
{
	unordered_map<string,enum FIELD_TYPE> colType;
	unordered_map<string, Field> colValues;
	CondEval whereCondEval;
	bool whereExists, selectTuple;

	Schema joinedSchema, outputSchema, rel1Schema, rel2Schema;
	string joinedName, outputName;
	Relation *outputRel, *joinedRel;

	vector<Tuple> rel1Tuples, rel2Tuples;
	int rel2MemIdx, outputMemIdx, startDiskIdx, endDiskIdx;
	int currDiskBlock;
	int rel1NumBlocks, rel2NumBlocks, subListSize;
	int freeMemBlocks, k;

	// Error Checking:
	if(rel1 == NULL || rel2==NULL)
		return NULL;

	rel1NumBlocks = rel1->getNumOfBlocks();
	rel2NumBlocks = rel2->getNumOfBlocks();
	if(rel1NumBlocks == 0 || rel2NumBlocks == 0)
		return NULL;
	
	// Step 1: process schemas
	rel1Schema = rel1->getSchema();
	rel2Schema = rel2->getSchema();
	joinedSchema = joinSchemas(rel1Schema, rel2Schema,rel1,rel2);
	joinedName = rel1->getRelationName() + "_" + rel2->getRelationName() + "_joined";
	joinedRel = schema_manager.createRelation(joinedName, joinedSchema);
	tempRelations.push_back(joinedRel);
	
	outputSchema = getOutputSchema(joinedSchema, projectionSet);

	outputName = rel1->getRelationName() + "_" + rel2->getRelationName() + "_output";
	outputRel = schema_manager.createRelation(outputName, outputSchema);
	tempRelations.push_back(outputRel);

	resultInMemory = false;

	// Step 2: process where condition
	if(whereString.length()>0)
		whereExists = true;
	else
		whereExists = false;

	// initialize where condition evaluator
	if(whereExists)
	{
		schemaToMap(colType, joinedSchema);
		whereCondEval.intialize(whereString,colType);
	}

	// Step 3: Setup Memories
	buffer_manager.getAllFreeBlocks(memoryBlockIndices);
	rel2MemIdx = memoryBlockIndices.back();
	memoryBlockIndices.pop_back();
	outputMemIdx = memoryBlockIndices.back();
	memoryBlockIndices.pop_back();

	// Step 5: Perform Join
	currDiskBlock = 0;
	freeMemBlocks = memoryBlockIndices.size();
	subListSize = (rel1NumBlocks -1)/freeMemBlocks +1;
	for(int i = 0; i<subListSize;i++)
	{
		startDiskIdx = i*freeMemBlocks;
		endDiskIdx = startDiskIdx+ freeMemBlocks -1;
		if(endDiskIdx > (rel1NumBlocks-1))
			endDiskIdx = rel1NumBlocks-1;
		k =0;
		for(int j =startDiskIdx;j<=endDiskIdx;j++)
		{
			rel1->getBlock(j, k);
			k++;
		}
		// get Rel1 Tuples
		rel1Tuples = mem->getTuples(0,endDiskIdx - startDiskIdx + 1);
		for(Tuple &rel1Tuple: rel1Tuples)
		{
			for(int l = 0;l<rel2NumBlocks;l++)
			{
				rel2->getBlock(l,rel2MemIdx);
				rel2Tuples = mem->getTuples(rel2MemIdx,1);

				// join and store the tuples
				for(Tuple &rel2Tuple:rel2Tuples)
				{
					Tuple joinedTuple = joinTuples(rel1Tuple, rel2Tuple, joinedRel);
					if(whereExists)
					{
						tupleToMap(joinedSchema, joinedTuple, colValues);
						if(!(whereCondEval.evaluate(colValues)))
							selectTuple = false;
						else
							selectTuple = true;
					}
					else
						selectTuple = true;
					if(selectTuple)
					{
						Tuple outTuple = outputRel->createTuple();
						outTuple = projectTuple(joinedTuple,outputRel);					
						appendTupleToRelation(outputRel,mem,outputMemIdx, outTuple);
					}
				}
			}
		}


	}
	// Step 6: Release all the memory
	memoryBlockIndices.push_back(outputMemIdx);
	memoryBlockIndices.push_back(rel2MemIdx);
	buffer_manager.releaseBulkIdx(memoryBlockIndices);
	buffer_manager.sort();

	return outputRel;
}
// Main orderby function
Relation* DatabaseEngine::execOrderBy(Relation *rel, string colName)
{
	int numBlocks, numMemBlocks;
	string tempName, finalname;
	vector<string> field_names;
	vector<enum FIELD_TYPE> field_types;
	enum FIELD_TYPE field_type;
	Relation *returnRel, *tempRel;
	Schema schema = rel->getSchema();
	field_names = schema.getFieldNames();
	field_types = schema.getFieldTypes();

	numBlocks = rel->getNumOfBlocks();
	numMemBlocks = buffer_manager.getFreeBlocksCount();
	cout<<"Actual MemBlocks: "<< mem->getMemorySize()<<endl;
	cout<<"Free blocks: "<<numMemBlocks<<endl;
	for(int i = 0; i<field_names.size();i++ )
	{
		if(field_names[i] == colName)
		{
			field_type = field_types[i];
			break;
		}
	}
	if(resultInMemory)
	{
		// direct one pass
		onePassSort(mem, memoryBlockIndices, colName,field_type);
		returnRel = rel;
	}
	else if(numBlocks <= numMemBlocks)
	{
		// Not in memory so load it in memory and call one pass sort
		resultInMemory = true;

		// load all blocks in memory
		for(int i = 0;i<numBlocks;i++)
		{
			int freeMemIdx = buffer_manager.getFreeBlockIdx();
			rel->getBlock(i,freeMemIdx);
			memoryBlockIndices.push_back(freeMemIdx);

		}
		onePassSort(mem, memoryBlockIndices, colName,field_type);
		returnRel = rel;

	}
	else if(((numMemBlocks-1)*numMemBlocks) > numBlocks)
	{
		finalname = rel->getRelationName() + "_Sorted_Output";
		returnRel = schema_manager.createRelation(finalname,schema);
		tempName = rel->getRelationName() + "_Sorted_temporary_out";
		tempRel = schema_manager.createRelation(tempName,schema);
		twoPassSort(rel, tempRel, returnRel, mem, buffer_manager,colName,field_type);
		tempRelations.push_back(returnRel);
		tempRelations.push_back(tempRel);
	}
	else
	{
		log("Cannot perform 2 pass sort as numBlocks in realtion is greater than total memory");
		return NULL;
	}

	return returnRel;
}

// Main Distinct function
Relation* DatabaseEngine::execDistinct(Relation *rel, string colName)
{
	int numBlocks, numMemBlocks;
	vector<string> field_names;
	vector<enum FIELD_TYPE> field_types;
	enum FIELD_TYPE field_type;
	Relation *returnRel, *tempRel;
	string tempName, finalname;
	Schema schema = rel->getSchema();
	field_names = schema.getFieldNames();
	field_types = schema.getFieldTypes();

	numBlocks = rel->getNumOfBlocks();
	numMemBlocks = buffer_manager.getFreeBlocksCount();
	cout<<"Actual MemBlocks: "<< mem->getMemorySize()<<endl;
	cout<<"Free blocks: "<<numMemBlocks<<endl;
	for(int i = 0; i<field_names.size();i++ )
	{
		if(field_names[i] == colName)
		{
			field_type = field_types[i];
			break;
		}
	}
	if(resultInMemory)
	{
		// direct one pass
		onePassRemoveDups(mem,memoryBlockIndices,buffer_manager,colName,field_type);
		returnRel = rel;
	}
	else if(numBlocks <= numMemBlocks)
	{
		// Not in memory so load it in memory and call one pass sort
		resultInMemory = true;

		// load all blocks in memory
		for(int i = 0;i<numBlocks;i++)
		{
			int freeMemIdx = buffer_manager.getFreeBlockIdx();
			rel->getBlock(i,freeMemIdx);
			memoryBlockIndices.push_back(freeMemIdx);

		}
		onePassRemoveDups(mem,memoryBlockIndices,buffer_manager,colName,field_type);
		returnRel = rel;

	}
	else if(((numMemBlocks-1)*numMemBlocks) > numBlocks)
	{
		finalname = rel->getRelationName() + "_Distinct_Output";
		returnRel = schema_manager.createRelation(finalname,schema);
		tempName = rel->getRelationName() + "_distinct_temporary_out";
		tempRel = schema_manager.createRelation(tempName,schema);
		twoPassRemoveDups(rel, tempRel, returnRel, mem, buffer_manager,colName,field_type);
		tempRelations.push_back(returnRel);
		tempRelations.push_back(tempRel);
	}
	else
	{
		log("Cannot perform 2 pass distcint as numBlocks in realtion is greater than total memory");
		return NULL;
	}

	return returnRel;
}
// Called after select query is executed
void DatabaseEngine::cleanUp()
{
	for(Relation *tempRelation: tempRelations)
		if(tempRelation!=NULL)
			schema_manager.deleteRelation(tempRelation->getRelationName());
	tempRelations.clear();
	buffer_manager.releaseBulkIdx(memoryBlockIndices);
	memoryBlockIndices.clear();
	buffer_manager.sort();
	return;	
}

// logger functions to log output to std out
// TODO: log to a file after all queries are implemented

void DatabaseEngine::log(Schema *schema)
{
	vector<string> field_names;
	field_names = schema->getFieldNames();

	for(string &str:field_names)
		cout<<str<<" ";
	cout<<endl;
}

void DatabaseEngine::log(Tuple &tuple)
{
	cout<<tuple<<endl;
	return;
}


void DatabaseEngine::log(Relation *relation_ptr)
{
	Block *memBlockPtr;
	int freeMemIdx, numBlocks;
	vector<Tuple> tuples;
	if(relation_ptr == NULL)
		return;
	Schema schema = relation_ptr->getSchema();
	log(&schema);

	if(resultInMemory)
	{

		tuples = mem->getTuples(memoryBlockIndices[0],memoryBlockIndices.size());	
		for(Tuple &t: tuples)
			log(t);
	}
	else
	{
		numBlocks = relation_ptr->getNumOfBlocks();
		freeMemIdx = buffer_manager.getFreeBlockIdx();

		for(int i= 0; i<numBlocks;i++)
		{
			memBlockPtr = mem->getBlock(freeMemIdx);
			memBlockPtr->clear();
			//load the memInBlock
			relation_ptr->getBlock(i,freeMemIdx);
			tuples = memBlockPtr->getTuples();
			for(Tuple &t: tuples)
				log(t);

		}
		buffer_manager.storeFreeBlockIdx(freeMemIdx);
	}

}

void DatabaseEngine::log(string value)
{
	cout<<value<<endl;
	return;
}


/*--------------Public Class Functions-------------*/

// Constructor to initilaize the databaseEngine with a memory and a disk
// This in turn initializes schema manager and buffer manager
DatabaseEngine::DatabaseEngine(MainMemory *mem, Disk *disk):schema_manager(mem,disk), buffer_manager(mem)
{
	this->mem = mem;
	this->disk = disk;
	resultInMemory = true;
	memoryBlockIndices.clear();
}


// the main functions which takens in raw query as input
// returns"SUCCESS" for successful query execution
// returns "FAILED" if any the query execution fails at any point
void DatabaseEngine::execQuery(string query)
{
	Node *root;
	enum NODE_TYPE nodeType;
	string returnCode = "FAILED";
	Relation* rel;
	// parse the query and create a parse tree

	root = parseQuery(query);
	if(root == NULL)
	{
		log("Error in parsing query:->"+query);
		log("Query Exec Status: "+returnCode);
		return;
	}

	// reset the disk ios and timers
	disk->resetDiskIOs();
    disk->resetDiskTimer();
	

    log("\n");
    log("************************************************************");
    log("Input Query:->  "+query);

    //execute the query
    nodeType = root->nodeType;
    switch(nodeType)
    {
    	case NODE_TYPE::CREATE_QUERY:
    		returnCode = execCreateQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::INSERT_QUERY:
    		returnCode = execInsertQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::DROP_QUERY:
    		returnCode = execDropQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::DELETE_QUERY:
    		returnCode = execDeleteQuery(root) ? "SUCCESS" : "FAILED";
    	break;
    	case NODE_TYPE::SELECT_QUERY:
    		rel = execSelectQuery(root, true);
    		returnCode = (rel==NULL)?"FAILED":"SUCCESS";
    		cleanUp();
    	break;
    	default:
    		returnCode = "FAILED";
    	break;
    }

    log("Query Exec Status: "+returnCode);
    log("\n");

    // record disk IOs and 
    log("Disk I/O count: " + std::to_string(disk->getDiskIOs()));
    log("Query Exec Time: " + std::to_string(disk->getDiskTimer()) + " ms");
    log("************************************************************");
    log("\n");
	delete root;
}