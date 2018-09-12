#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <io.h>
#include <math.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"

Schema *schema;
char *schema_str;

typedef struct tableInfo{
	int recordLength;	//record length
	int numTuples;	//tootal number of tuples in one table
	int totalPages;	//number of pages to store one table
	int pageMaxRecords;	//max number of records in one page
	Schema *t_schema;
	char *t_schema_str;	//schema string
	BM_BufferPool *bm;
	int firstFreePage;	//numPage of first page with free space
}tableInfo;

typedef struct ScanHandle{
	int curPage;	//current page of the scanning in table
	int curSlot;	//current slot of the scanning in table
	int totalPages;	//total number of pages
	int totalSlots;	//total slots in one page
	int recordSize;	//size of one record
	Expr *cond;	//scan condition
	BM_PageHandle *page;	//current page's data
	BM_BufferPool *bm;	//buffer pool
	RM_TableData *rel;	//table pointer
}ScanHandle;




RC getSchemaSize(Schema *schema){
	int i,size = 0;

	size = sizeof(int)	//numArr
		+ sizeof(int) * (schema->numAttr)	//dataTypes
		+ sizeof(int) * (schema->numAttr)	//type_lengths
		+ sizeof(int)						//keySize
		+ sizeof(int) * (schema->keySize);	//keyAttrs

	for(i = 0; i < schema->numAttr; i++){
		size += strlen(schema->attrNames[i]);
	}

	return size;
}

/*RC schemaToString(Schema *schema, char *page){
	int offset = 0;
	size_t attrLens; //length of an attribute

	//set numAttr
	memcpy(page+offset, &(schema->numAttr), sizeof(int));
	offset += sizeof(int);

	//set attriNames	
	for (int i = 0; i < schema->numAttr; i++) {
		attrLens = strlen(schema->attrNames[i])+1;	//get the length of an attribute
		memcpy(page + offset, schema->attrNames[i], attrLens);
		*(page + offset + attrLens) = '\0';		//set NULL terminator, in case
		offset += attrLens;
	}

	//set dataTypes
	for (int i = 0; i < schema->numAttr; i++) {
		memcpy(page + offset, &(schema->dataTypes[i]), sizeof(DataType));
		offset += sizeof(DataType);
	}

	//set typeLength
	for (int i = 0; i < schema->numAttr; i++) {
		memcpy(page + offset, &(schema->typeLength[i]), sizeof(int));
		offset += sizeof(int);
	}

	//set the keySize
	//set the keySize firstly
	memcpy(page + offset, &(schema->keySize), sizeof(int));
	offset += sizeof(int);

	//set the keyAttrs
	for (int i = 0; i < schema->keySize; i++) {
		memcpy(page + offset, &(schema->keyAttrs[i]), sizeof(int));
		offset += sizeof(int);
	}

	return RC_OK;
}*/


// table and manager
RC initRecordManager(void *mgmtData){
	return RC_OK;
}

RC shutdownRecordManager(){
	return RC_OK;
}

RC createTable(char *name, Schema *schema){
	int message;
	int offset = 0;
	int schema_size;
	int record_size;
	int total_pages;
	int max_records;
	int ffp;
	tableInfo *tInfo = (tableInfo *)malloc(sizeof(tableInfo));	
	//initialize  buffer manager
	BM_BufferPool *bm = MAKE_POOL();
	//create pageHandle to store info data
	BM_PageHandle *bp_0 = MAKE_PAGE_HANDLE();

	tableInfo *ti = (tableInfo *) bp_0->data; // create a pointer with type tableInfo * to store table info

	// Set the name of the file to the Buffer Manager
	bm->pageFile = name;

	//if  the schema string is empty, return an error message
	if(schema == NULL || strlen(schema_str) == 0){
		return RC_SCHEMA_NOT_CREATED;
	}
	//create page file
	if(message = createPageFile(name) != RC_OK){
		return message;
	}
	//initialize the number of page in buffer as 3 using LRU strategy
	initBufferPool(bm, name, 3, RS_LRU, NULL);

	//create one empty page to store table info
	//char *infoPage = (char*)malloc(PAGE_SIZE * sizeof(char));
	//memset(infoPage,'\0', PAGE_SIZE);

	schema_str = serializeSchema( schema);	//get schema string

	schema_size = getSchemaSize(schema);
	record_size = getRecordSize(schema);
	record_size += (sizeof(char) + sizeof(int));	//char for '\n', int for tombstone, marking dirty
	total_pages = (int) ceil((float)schema_size/PAGE_SIZE);
	max_records = (int) floor((float)PAGE_SIZE/(float)record_size);

	//set firs page number with free space
	if(schema_size % PAGE_SIZE == 0){
		ffp = total_pages + 1;
	}
	else
		ffp = total_pages;

	//create first page with tableInfo	
	tInfo->bm = bm;
	tInfo->firstFreePage = ffp;
	tInfo->numTuples = 0;
	tInfo->pageMaxRecords = max_records;
	tInfo->recordLength = record_size;
	tInfo->totalPages = total_pages;
	tInfo->t_schema = schema;
	tInfo->t_schema_str = schema_str;

	ti = tInfo;	///???????
	pinPage(bm, bp_0, 0);

	//page 0 store tableInfo
	if(message = markDirty(bm, bp_0) != RC_OK){
		return message;
	}
	if(message = unpinPage(bm, bp_0) != RC_OK){
		return message;
	}
	if(message = shutdownBufferPool(bm) != RC_OK){
		return message;
	}

	free(bp_0);
	free(bm);

	return RC_OK;
}

RC openTable (RM_TableData *rel, char *name){
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *page = MAKE_PAGE_HANDLE();
	
	tableInfo *tInfo = (tableInfo*)page->data;

	initBufferPool(bm, name, 3, RS_LRU, NULL);

	pinPage(bm, page, 0);

	rel->mgmtData = tInfo;
	rel->name = name;
	rel->schema = schema;

	free(page);
	free(bm);

	return RC_OK;
}

RC closeTable (RM_TableData *rel){
	tableInfo *tInfo = (tableInfo *)rel->mgmtData;
	BM_BufferPool *bm = (BM_BufferPool *)tInfo->bm;

	freeSchema(rel->schema);
	shutdownBufferPool(bm);
	free(tInfo);

	return RC_OK;

}

RC deleteTable (char *name){
	int message;
	if(message = destroyPageFile(name) != RC_OK){
		return message;
	}
	return RC_OK;
}

int getNumTuples (RM_TableData *rel) {
	//return the tuple numbers
	tableInfo *tInfo = (tableInfo *)rel->mgmtData;
	return tInfo->numTuples;
}


// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
	int pageNum;	//the page number will be used for insertion
	int slot;	//offset from the start location of the page

	tableInfo *tInfo = (tableInfo *)rel->mgmtData;
	BM_BufferPool *bm = tInfo->bm;
	char *tempRecord = (char *)malloc(sizeof(tInfo->recordLength));

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	if(tInfo->recordLength == 0){
		return RC_FILE_NOT_FOUND;
	}

	//pageNum = tInfo->numTuples/tInfo->pageMaxRecords + 1;	//page 0 stores the table info
	pageNum = tInfo->firstFreePage;

	if(tInfo->numTuples % tInfo->pageMaxRecords == 0){
		slot = 0;	//need a new page
		tInfo->firstFreePage++;	//set firstFreePage + 1
		pageNum++;
		tInfo->totalPages++;
	}
	else{
		slot = (tInfo->numTuples % tInfo->pageMaxRecords) * tInfo->recordLength;
	}

	page->pageNum = pageNum;
	pinPage(bm, page, pageNum);

	record->id.page = pageNum;
	record->id.slot = slot;

	*((int *)tempRecord) = 0;	//set tombstone 0, undeleted
	memcpy(tempRecord + sizeof(int), record->data, tInfo->recordLength - sizeof(int) - sizeof(char));
	*((char *) tempRecord + tInfo->recordLength - sizeof(char)) = '\0';

	//insert the record to the page offset the slot
	memcpy(page->data + slot, tempRecord, tInfo->recordLength);

	markDirty(bm, page);
	forcePage(bm, page);
	unpinPage(bm, page);

	tInfo->numTuples++;
	
	free(tempRecord);

	return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){
	tableInfo *tInfo = (tableInfo *)rel->mgmtData;
	BM_BufferPool *bm = tInfo->bm;

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	pinPage(bm, page, id.page);

	*((int*)(page->data) + id.slot) = 1;	//set this record as tombstone, 1 indicates it being deleted

	markDirty(bm, page);
	unpinPage(bm, page);

	tInfo->numTuples --;

	return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record){
	tableInfo *tInfo = (tableInfo *)rel->mgmtData;
	BM_BufferPool *bm = tInfo->bm;

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	pinPage(bm, page, record->id.page);

	//update the record
	memcpy((page->data) + record->id.slot + sizeof(int), record->data, tInfo->recordLength - sizeof(int) - sizeof(char));

	markDirty(bm, page);
	unpinPage(bm, page);

	return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record){
	tableInfo *tInfo = (tableInfo *)rel->mgmtData;
	BM_BufferPool *bm = tInfo->bm;

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	pinPage(bm, page, id.page);

	//check if the record have already deleted, check tombstone flag
	if(*(int *)((page->data) + id.slot) == 1){
		return RC_READ_NON_EXISTING_RECORD;
	}

	memcpy(record->data, (page->data) + id.slot + sizeof(int), tInfo->recordLength - sizeof(int) - sizeof(char));

	record->id.page = id.page;
	record->id.slot = id.slot;

	unpinPage(bm, page);

	return RC_OK;
}


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	tableInfo *tInfo = (tableInfo *)rel->mgmtData;
	BM_BufferPool *bm = tInfo->bm;
	ScanHandle *sh = (ScanHandle*)malloc(sizeof(ScanHandle));
	//BM_MgmtData *bmmgmt = (BM_MgmtData *) bm->mgmtData;

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	pinPage(bm, page, 1);

	if(getRecordSize(rel->schema) == 0){
		return RC_FILE_NOT_FOUND;
	}

	sh->curPage = 1;
	sh->curSlot = 0;
	sh->cond = cond;
	sh->rel = rel;
	sh->bm = bm;
	sh->page = page;
	sh->totalPages = tInfo->totalPages;
	sh->totalSlots = tInfo->pageMaxRecords * tInfo->recordLength;
	sh->recordSize = tInfo->recordLength;

	unpinPage(bm, page);

	scan->rel = rel;
	scan->mgmtData = sh;

	return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record){
	int message;
	ScanHandle *sh = (ScanHandle *) scan->mgmtData;	
	Value *val = (Value *)malloc(sizeof(Value));

	//no more records, scan is complete
	if(sh->curPage == sh->totalPages - 1 && sh->curSlot == sh->totalSlots){
		return RC_READ_NON_EXISTING_RECORD;
	}

	//scan to the end of one page, move to the next page
	if(sh->curSlot == sh->totalSlots){
		sh->curPage++;
		sh->curSlot = 0;
		pinPage(sh->bm, sh->page, sh->curPage);
		unpinPage(sh->bm, sh->page);
	}

	//current record has been deleted, skip it
	if(*(int *)(sh->page + sh->curSlot) == 1){
		sh->curSlot += sh->recordSize;
		message = next(scan, record);
	}

	//condition is null, return all tuples
	if(sh->cond == NULL){
		record->id.page = sh->curPage;
		record->id.slot = sh->curSlot;

		memcpy(record->data, sh->page->data + sh->curSlot + sizeof(int), sh->recordSize - sizeof(int));
		sh->curSlot += sh->recordSize;
		scan->mgmtData = sh;
		return RC_OK;
	}

	//scan with condition, check the condition
	else{
		record->id.page = sh->curPage;
		record->id.slot = sh->curSlot;

		memcpy(record->data, sh->page->data + sh->curSlot + sizeof(int), sh->recordSize - sizeof(int));
		sh->curSlot += sh->recordSize;
		scan->mgmtData = sh;

		//check whether this record satisfy the scan condition
		evalExpr(record, sh->rel->schema, sh->cond, &val);

		if(val->v.boolV)
			return RC_OK;
		else
			message = next(scan, record);
	}

	return message;
}

RC closeScan (RM_ScanHandle *scan){
	ScanHandle *sh = (ScanHandle *) scan->mgmtData;
	free(sh->page);
	free(sh);
	
	return RC_OK;
}


// dealing with schemas
RC getRecordSize(Schema *schema){
	int i,len = 0;
	DataType *dt = schema->dataTypes;

	if(!schema){
		return RC_SCHEMA_NOT_CREATED;
	}

	for(i = 0; i< schema->numAttr; i++){
		if(dt[i] == DT_INT){
			len += sizeof(int);
		}
		else if(dt[i] == DT_FLOAT){
			len += sizeof(float);
		}
		else if(dt[i] == DT_BOOL){
			len += sizeof(bool);
		}
		else if(dt[i] == DT_STRING){
			len += (schema->typeLength[i] * sizeof(char));
		}
	}

	return len;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
	schema = (Schema *)malloc(sizeof(Schema));
	
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->keyAttrs = keys;
	schema->keySize = keySize;
	schema->numAttr = numAttr;
	schema->typeLength = typeLength;

	return schema;
}

RC freeSchema (Schema *schema){
	int i;
	//free all memory which is allocated to schema
	for (i = 0; i < schema->numAttr; i++) {
		free(schema->attrNames[i]);
	}
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);

	free(schema);

	return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){

	//allocate memory to record and its data
	*record = (Record*)malloc(sizeof(Record));
	(*record)->data = (char *)malloc(getRecordSize(schema));

	return RC_OK;
}

extern RC freeRecord (Record *record){

	//free memory which is allocated to record and record data
	free(record->data);
	free(record);

	return RC_OK;
}

RC getAttrOffset(Schema *schema, int attrNum){
	int i, pos = 0;
	DataType *dt = schema->dataTypes;

	if(!schema){
		return RC_SCHEMA_NOT_CREATED;
	}

	for(i = 0; i < attrNum; i++){
		if(dt[i] == DT_INT){
			pos += sizeof(int);
		}
		else if(dt[i] == DT_FLOAT){
			pos += sizeof(float);
		}
		else if(dt[i] == DT_BOOL){
			pos += sizeof(bool);
		}
		else if(dt[i] == DT_STRING){
			pos += (schema->typeLength[i] * sizeof(char));
		}
	}

	return pos;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	int len, pos = 0;
	DataType *dt;
	Value *val;
	dt = schema->dataTypes;	
	val = (Value *)malloc(sizeof(Value));

	//get offset of Attr
	pos = getAttrOffset(schema, attrNum);	
	// data type is not defined in schema
	if(pos == 0){
		return RC_DATATYPE_ERROR;
	}

	val->dt = dt[attrNum];	//set value data type

	switch(val->dt){
	case DT_INT:	//data type int
		memcpy(&(val->v.intV), record->data + pos, sizeof(int));
		break;

	case DT_STRING:	//data type string
		len = schema->typeLength[attrNum];
		val->v.stringV = (char*)malloc(sizeof(char) * (len + 1));	//reserve one space for '\0'
		strncpy(val->v.stringV, record->data + pos, len);
		val->v.stringV[len] = '\0';	//add ending character for the string
		break;

	case DT_FLOAT:	//data type float
		memcpy(&(val->v.floatV), record->data + pos, sizeof(float));
		break;

	case DT_BOOL:	//data type bool
		memcpy(&(val->v.boolV), record->data + pos, sizeof(bool));
		break;

	default:
		return RC_DATATYPE_ERROR;
	}

	*value = val;
	return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	int len,pos = 0;
	DataType *dt = schema->dataTypes;

	//get offset of Attr
	pos = getAttrOffset(schema, attrNum);	
	// data type is not defined in schema
	if(pos == 0){
		return RC_DATATYPE_ERROR;
	}

	switch(dt[attrNum]){
	case DT_INT:		//data type int
		memcpy(record->data + pos, &(value->v.intV), sizeof(int));
    	break;

    case DT_STRING:		//data type string
    	len = schema->typeLength[attrNum];
    	strncpy(record->data + pos, value->v.stringV, len);		//no '\0' will be copied
    	break;

    case DT_FLOAT:		//data type float
    	memcpy(record->data + pos, &(value->v.floatV), sizeof(float));
    	break;

    case DT_BOOL:		//data type bool
    	memcpy(record->data + pos, &(value->v.boolV), sizeof(bool));
    	break;
    	
	default:		
			return RC_DATATYPE_ERROR;
	}

	return RC_OK;
}










