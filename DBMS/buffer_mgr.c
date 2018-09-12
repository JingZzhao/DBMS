#include <stdio.h>
#include <io.h>
#include <malloc.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"

/*define an element(one page frame) in page frame list*/
typedef struct pageFrame{
	//char key;
	BM_PageHandle BM_P;	//include the pageNum of page in the page file,and actual data
	int frameNum;	//the index of the node in the frame list
	int pin_count;	//how many clinets are using working with this page
	bool dirty;		//if it is TRUE, the page has been modified

	//struct pageFrame *hashListPrev;	//hash map pointer points to the previous element
	//struct pageFrame *hashListNext;	//hash map pointer points to the next element

	struct pageFrame *listPrev;	//double linked list pointer points to the previous element
	struct pageFrame *listNext;	//double linked list pointer points to the next element
}pageFrame;

/*define a page frame double linked list in buffer pool*/
typedef struct frameList{
	int listCapacity;	//the capacity of list
	//pageFrame **hashMap;	//hash map of list

	pageFrame *listHead;	//head of list
	pageFrame *listTail;	//tail of list
	int listSize;	//the number of noeds in list
}frameList;

typedef struct mgmtData{
	int numFrames;	//the number of page frames
	int numRead;	//the number of pages that have been read from disk
	int numWrite;	//the number of pages written to the page file
	int numPinning;	//total number of pining pages
	int numDirtyPages;	//total number of dirty pages
	int fList[200];	//an array of pageNum in the frame list
	bool dirtyList[200];
	int pinCountList[200];
	void *stratData;
	frameList *fl;
}mgmtData;

/*create a new page frame*/
static pageFrame* newPageFrame(){
	pageFrame* newpf = (pageFrame*)malloc(sizeof(pageFrame));
	newpf->BM_P.pageNum = NO_PAGE;
	newpf->BM_P.data = (char*)malloc(PAGE_SIZE * sizeof(SM_PageHandle));

	newpf->frameNum = 0;
	newpf->dirty = FALSE;
	newpf->pin_count = 0;	
	newpf->listPrev = NULL;
	newpf->listNext = NULL;

	return newpf;
}

/*free a page frame*/
static void freePageFrame(pageFrame* onepf){
	if(NULL == onepf) return;
	free(onepf);
}

/*Double linked list interface and implementation*/
/*remove a specific page frame*/
void removeFromFrameList(frameList *fl, pageFrame *pf){
	//frame list is NULL
	if(fl->listSize == 0){
		return;
	}

	else{
		if(pf->listNext == NULL){		//removed page is at tail	
			fl->listTail = pf->listPrev;
			pf->listPrev->listNext = NULL;
			if(fl->listSize == fl->listCapacity){
				(fl->listSize)--;
			}
		}
		else{
			pf->listPrev->listNext = pf->listNext;
			pf->listNext->listPrev = pf->listPrev;
			if(fl->listSize == fl->listCapacity){
				fl->listSize--;
			}
		}
	}	
}

/*insert one page fram into the head of list*/
pageFrame *insertToListHead(BM_BufferPool *const bm, BM_PageHandle *const page, pageFrame *pf){
	//int message;
	frameList *fl;
	mgmtData *bufferPoolInfo; 
	//pageFrame *onepf;
	pageFrame *removedPF;
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = (frameList *)bufferPoolInfo->fl;	
	removedPF = pf;

	if(fl->listSize == 0){
		//list is empty
		fl->listHead = fl->listTail = pf;
		(fl->listSize)++;
		bufferPoolInfo->numFrames++;
	}
	else{
		//the frame list is full	
		if(fl->listSize  == fl->listCapacity){
			if(removedPF != NULL){
				if(removedPF->dirty && removedPF->pin_count == 0){
					if(forcePage(bm, &removedPF->BM_P) != RC_OK ){
						return NULL;
					}
				}
				while(removedPF->pin_count != 0){
					if(removedPF->listPrev == NULL){
						return NULL;
					}
					removedPF = removedPF->listPrev;
				}
				if(removedPF->dirty){
					if(forcePage(bm, &removedPF->BM_P) != RC_OK ){
						return NULL;
					}
				}
			}
		}
		removeFromFrameList(fl, removedPF);
		removedPF->listNext = fl->listHead;
		removedPF->listPrev = NULL;
		fl->listHead->listPrev = removedPF;
		fl->listHead = removedPF;		
		if(fl->listSize < fl->listCapacity){
			if(bufferPoolInfo->numFrames < fl->listCapacity){
				bufferPoolInfo->numFrames++;
			}
		}
		(fl->listSize)++;
	}

	return removedPF;
}

/*find the page frame by pageNum*/
pageFrame* findPageFrame(frameList *fl,  const PageNumber pageNum){
	pageFrame *pfCurrent;
	pfCurrent = fl->listHead;

	while(pfCurrent != NULL){
		if(pfCurrent->BM_P.pageNum == pageNum){
			return pfCurrent;
		}
		pfCurrent = pfCurrent->listNext;
	}
	return pfCurrent;
}


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData)
{
	int i;
	int openPF;
	SM_FileHandle SM_F;
	pageFrame *pf;
	frameList *framel;
	mgmtData *bufferPoolInfo; 

	bufferPoolInfo = (mgmtData*)malloc(sizeof(mgmtData));
	framel = (frameList*)malloc(sizeof(frameList));

	pf = newPageFrame();

	openPF = openPageFile((char *)pageFileName, &SM_F);	//open the page file

	if(numPages <= 0){
		closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}
	
	if(openPF != RC_OK){
		closePageFile(&SM_F);
		return openPF;	//the page file doesn't exist
	}

	//initialize the mgmtData
	bufferPoolInfo->numFrames = 0;
	bufferPoolInfo->numRead = 0;
	bufferPoolInfo->numWrite = 0;
	bufferPoolInfo->numPinning = 0;
	bufferPoolInfo->numDirtyPages = 0;
	bufferPoolInfo->stratData = stratData;
	bufferPoolInfo->fl = framel;	

	memset(bufferPoolInfo->fList,NO_PAGE,200*sizeof(int));
	memset(bufferPoolInfo->dirtyList,NO_PAGE,200*sizeof(bool));
	memset(bufferPoolInfo->pinCountList,NO_PAGE,200*sizeof(int));

	framel->listCapacity = numPages;
	framel->listSize = 0;
	framel->listHead = framel->listTail = pf;
	//create an empty frame list with capacity = numPages
	for(i = 1;i < numPages; i++){
		pf->listNext = newPageFrame();
		pf->listNext->listPrev = pf;
		pf = pf->listNext;
		pf->frameNum = i;
	}

	//initialize the buffer pool
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->mgmtData = bufferPoolInfo;	

	closePageFile(&SM_F);

	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
	int openPF;
	bool open_file;
	SM_FileHandle SM_F;
	pageFrame *pfCurrent;
	frameList *fl;
	mgmtData *bufferPoolInfo; 
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = (frameList *)bufferPoolInfo->fl;
	pfCurrent = (pageFrame *)fl->listHead;

	openPF = openPageFile(bm->pageFile, &SM_F);
	open_file = true;

	if(bm->numPages <= 0){
		closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}
	if(openPF != RC_OK){
		closePageFile(&SM_F);
		return openPF;	
	}

	if(bufferPoolInfo->numPinning > 0){
		closePageFile(&SM_F);
		return RC_SHUTDOWN_FAILED;	//buffer pool has pinned pages
	}
	//write all dirty pages back to disk
	if(bufferPoolInfo->numDirtyPages > 0){
		forceFlushPool(bm);
	}

	//free the memory allocated for page frames
	while(pfCurrent != NULL){
		pfCurrent = pfCurrent->listNext;
		free(fl->listHead->BM_P.data);
		free(fl->listHead);
		fl->listHead = pfCurrent;
	}
	fl->listHead = fl->listTail = NULL;
	free(fl);	//free the memory allocated for frame list
	free(bufferPoolInfo);	//free the memory allocated for mgmtData	
	bm->numPages = 0;

	closePageFile(&SM_F);// close the open file
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
	int i;
	int openPF;
	SM_FileHandle SM_F;
	pageFrame *pfCurrent;
	frameList *fl;
	mgmtData *bufferPoolInfo; 
	openPF = openPageFile(bm->pageFile, &SM_F);
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = (frameList *)bufferPoolInfo->fl;
	pfCurrent = (pageFrame *)fl->listHead;

	if(bm->numPages <= 0){
		closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}
	if(openPF != RC_OK){
		closePageFile(&SM_F);
		return openPF;	
	}

	//write all dirty pages back to disk
	while(pfCurrent != NULL){
		if(pfCurrent->dirty && pfCurrent->pin_count == 0){
			forcePage(bm, &pfCurrent->BM_P);	
		}
		pfCurrent = pfCurrent->listNext;
	}

	closePageFile(&SM_F);
	return RC_OK;
}

RC pinPage_FIFO (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum){
	int i;
	int message;
	SM_FileHandle SM_F;
	pageFrame *pf;
	pageFrame *removedPF;
	frameList *fl;
	mgmtData *bufferPoolInfo; 
	message = openPageFile(bm->pageFile, &SM_F);
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = (frameList *)bufferPoolInfo->fl;

	if(bm->numPages <= 0){
		closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}
	if(message != RC_OK){
		closePageFile(&SM_F);
		return message;	
	}

	/*check if the page frame in the frame list*/
	pf = findPageFrame(fl, pageNum);
	if(pf != NULL){
		if(pf->pin_count == 0){
			bufferPoolInfo->numPinning++;
		}
		(pf->pin_count)++;
		closePageFile(&SM_F);
		return RC_OK;
	}

	//locate the first free page frame from the head
	pf = fl->listHead;
	for(i = 0; i < fl->listSize; i++){
		if(pf->listNext != NULL){
		 pf = pf->listNext;	//if listSize = listCapacity, pf points to the tail of the frame list, frame list is full
		}
	}

	pf = insertToListHead(bm, page, pf);	

	if(message = ensureCapacity(pageNum, &SM_F) != RC_OK){
		return message;
	}//increase the capacity of the page file
	
	page->pageNum = pageNum;
	page->data = (char*)malloc(PAGE_SIZE);	

	//read block from disk
	message = readBlock(pageNum, &SM_F, page->data);
	if(message != RC_OK){
		closePageFile(&SM_F);
		return message;
	}		
	

	pf->BM_P.pageNum = pageNum;
	pf->BM_P.data = page->data;
	pf->dirty = FALSE;
	(pf->pin_count)++;
	
	bufferPoolInfo->fList[pf->frameNum] = pf->BM_P.pageNum;
	(bufferPoolInfo->numRead)++; 	
	
	// if frame list is not full, increase the numFrames
	(bufferPoolInfo->numPinning)++;
	
	return RC_OK;	
}

RC pinPage_LRU (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum){			
	int i;
	int message;
	SM_FileHandle SM_F;
	pageFrame *pf;
	pageFrame *removedPF;
	frameList *fl;
	mgmtData *bufferPoolInfo; 
	message = openPageFile(bm->pageFile, &SM_F);
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = (frameList *)bufferPoolInfo->fl;

	if(bm->numPages <= 0){
		closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}
	if(message != RC_OK){
		closePageFile(&SM_F);
		return message;	
	}
	
	page->pageNum = pageNum;
	page->data = (char*)malloc(PAGE_SIZE);
	/*check if the page frame in the frame list*/
	pf = findPageFrame(fl, pageNum);
	if(pf != NULL){
		if(pf->pin_count == 0){
			bufferPoolInfo->numPinning++;
		}
		if(pf->listPrev != NULL){
			insertToListHead(bm, page, pf);	//different from FIFO
		}		
		(pf->pin_count)++;
		closePageFile(&SM_F);
		return RC_OK;
	}

	//locate the first free page frame from the head
	pf = fl->listHead;
	for(i = 0; i < fl->listSize; i++){
		if(pf->listNext != NULL){
		 pf = pf->listNext;	//if listSize = listCapacity, pf points to the tail of the frame list, frame list is full
		}
	}		

	pf = insertToListHead(bm, page, pf);	

	if(message = ensureCapacity(pageNum, &SM_F) != RC_OK){
		return message;
	}	//increase the capacity of the page file
	
	//read block from disk
	message = readBlock(pageNum, &SM_F, page->data);
	if(message != RC_OK){
		closePageFile(&SM_F);
		return message;
	}
	(bufferPoolInfo->numRead)++; 
	
	pf->BM_P.pageNum = pageNum;
	pf->BM_P.data = page->data;
	pf->dirty = FALSE;
	(pf->pin_count)++;
	bufferPoolInfo->fList[pf->frameNum] = pf->BM_P.pageNum;	
			
	// if frame list is not full, increase the numFrames
	(bufferPoolInfo->numPinning)++;
	closePageFile(&SM_F);
	
	return RC_OK;	
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum){
	if(bm->numPages <= 0){
		return RC_READ_NON_EXISTING_PAGE;
	}

	switch(bm->strategy)
	{
		case RS_FIFO:
			return pinPage_FIFO(bm, page, pageNum);
			break;
		case RS_LRU:
			return pinPage_LRU(bm, page, pageNum);
			break;
		default:
			return RC_UNKNOWN_STRATEGY;
			break;
	}

	return RC_OK;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	mgmtData *bufferPoolInfo; 
	pageFrame *pf;
	frameList *fl;
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = bufferPoolInfo->fl;
	pf = fl->listHead;

	if(bm->numPages <= 0){
		return RC_READ_NON_EXISTING_PAGE;
	}

	//find the page frame
	pf = findPageFrame(fl, page->pageNum);

	if(pf == NULL){
		return RC_READ_NON_EXISTING_PAGE;
	}

	//mark the page frame dirty
	pf->dirty = TRUE;
	(bufferPoolInfo->numDirtyPages)++;

	return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	int message;
	mgmtData *bufferPoolInfo; 
	pageFrame *pf;
	frameList *fl;
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = bufferPoolInfo->fl;
	pf = fl->listHead;

	if(bm->numPages <= 0){
		//closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}

	//find the page frame
	pf = findPageFrame(fl, page->pageNum);

	if(pf == NULL){
		return RC_READ_NON_EXISTING_PAGE;
	}
	
	//unpin the page frame
	if(pf->pin_count > 0){
		(pf->pin_count)--;	
		if(pf->pin_count == 0){
			if(pf->dirty){
				if(message = forcePage(bm, page) != RC_OK){
					return message;
				}
				pf->dirty = TRUE;
				bufferPoolInfo->numDirtyPages++;
				bufferPoolInfo->numWrite--;
			}
			(bufferPoolInfo->numPinning)--;
		}
	}	
	if(pf->pin_count < 0){
		return RC_READ_NON_EXISTING_PAGE;
	}

	return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	int message;
	mgmtData *bufferPoolInfo; 
	pageFrame *pf;
	frameList *fl;
	SM_FileHandle SM_F;
	message = openPageFile(bm->pageFile, &SM_F);
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = bufferPoolInfo->fl;
	pf = fl->listHead;

	if(message != RC_OK){
		closePageFile(&SM_F);
		return message;	
	}

	if(bm->numPages <= 0){
		closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}

	//find the page frame
	pf = findPageFrame(fl, page->pageNum);

	if(pf == NULL){
		closePageFile(&SM_F);
		return RC_READ_NON_EXISTING_PAGE;
	}

	if(pf->dirty && pf->pin_count == 0){	//write page frame back to disk
		message = writeBlock(page->pageNum, &SM_F, page->data );
		if(message != RC_OK){
			closePageFile(&SM_F);
			return message;
		}
		pf->dirty = FALSE;	
		(bufferPoolInfo->numDirtyPages)--;	
		(bufferPoolInfo->numWrite)++;
	}	
	if(pf->dirty && pf->pin_count != 0){
		closePageFile(&SM_F);
		return RC_WRITE_FAILED;
	}
	
	closePageFile(&SM_F);
	return RC_OK;
}


 // Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
	mgmtData *bufferPoolInfo; 
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	return bufferPoolInfo->fList;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
	mgmtData *bufferPoolInfo; 
	pageFrame *pf;
	frameList *fl;
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = bufferPoolInfo->fl;
	pf = fl->listHead;

	while(pf != NULL){
		bufferPoolInfo->dirtyList[pf->frameNum] = pf->dirty;
		pf = pf->listNext;
	}

	return bufferPoolInfo->dirtyList;
}

int *getFixCounts (BM_BufferPool *const bm){
	mgmtData *bufferPoolInfo; 
	pageFrame *pf;
	frameList *fl;
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	fl = bufferPoolInfo->fl;
	pf = fl->listHead;

	while(pf != NULL){
		bufferPoolInfo->pinCountList[pf->frameNum] = pf->pin_count;
		pf = pf->listNext;
	}

	return bufferPoolInfo->pinCountList;
}

int getNumReadIO (BM_BufferPool *const bm){
	mgmtData *bufferPoolInfo; 
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	return bufferPoolInfo->numRead;
}

int getNumWriteIO (BM_BufferPool *const bm){
	mgmtData *bufferPoolInfo; 
	bufferPoolInfo = (mgmtData *)bm->mgmtData;
	return bufferPoolInfo->numWrite;
}