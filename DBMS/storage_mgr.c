#include <stdio.h>
#include <io.h>
#include <malloc.h>
#include <stdlib.h>
#include "storage_mgr.h"

void initStorageManager (void){// initialized the storage menager
}

RC createPageFile (char *fileName){
	FILE *pfile = fopen(fileName, "r");
	SM_FileHandle *SM_F;
	char *firstp;

	SM_F = (SM_FileHandle*)calloc(PAGE_SIZE, sizeof(char)); // initialize
	firstp = (char *) calloc(PAGE_SIZE, sizeof(char));
	//SM_F->fileName = (char*)malloc(sizeof(char));

	if(pfile != NULL){
		printf("The file is exists.");
		fclose(pfile);
		return RC_FILE_EXISTS;
	}
	else{
		SM_F->fileName = fileName;
		SM_F->curPagePos = 0;
		SM_F->totalNumPages = 1;
		SM_F->mgmtInfo = NULL;

		pfile = fopen(fileName,"w+");

		fseek(pfile,0,SEEK_SET);//at the begining of the file
		fwrite(SM_F,sizeof(char),PAGE_SIZE,pfile);//save the information of fileHandle in first page
		fwrite(firstp,sizeof(char),PAGE_SIZE,pfile);// actual first page

		free(SM_F);
		free(firstp);

		fclose(pfile);
		return RC_OK;
	}
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
	FILE *pfile = fopen(fileName, "r");
	long int a;
	long int b;
	long int currP = 0;
	//int state = 1;
	if(pfile != NULL){
		//initializ the file handle with the information about the opened file
		fHandle->fileName = fileName;

		fseek(pfile, 0, SEEK_END);
		currP = ftell(pfile);
		a = fHandle ->totalNumPages = currP/PAGE_SIZE - 1;

		fseek(pfile, 0, SEEK_SET);
		currP = ftell(pfile);
		b = fHandle->curPagePos = currP/PAGE_SIZE;

		//fHandle->mgmtInfo = (void*)state;
		fHandle->mgmtInfo = (void*)pfile;

		return RC_OK;
	}
	else{
		return RC_FILE_NOT_FOUND;
	}
}

RC closePageFile (SM_FileHandle *fHandle){
	int check;
	FILE *fileBuff = (FILE*)fHandle->mgmtInfo;//open page file descriptor
	if(fileBuff == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else{
		check = fclose(fileBuff);
		if(!check){
			fHandle->mgmtInfo = NULL;
			return RC_OK;
		}
		else{
			return RC_FILE_NOT_FOUND;
		}
	}
}

RC destroyPageFile (char *fileName){
	int check;
	check = remove(fileName);
	if(check == 0){
		return RC_OK;
	}

	else{
		return RC_FILE_NOT_FOUND;
	}
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	FILE *fileBuff;

	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	if(fHandle->mgmtInfo != NULL ){
		closePageFile(fHandle);// close the open file
	}

	fileBuff = fopen(fHandle->fileName,"r");
	if(fileBuff == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else{
		if(fHandle->totalNumPages < pageNum || pageNum < 0){//the file has less than pageNum pages
			return RC_READ_NON_EXISTING_PAGE;
		}
		else{// read the pageNumth block from a file
			// seek to target page
			fseek(fileBuff,(pageNum + 1)*PAGE_SIZE,SEEK_SET);
			// store the content in the memory pointed to by the memPage page handle
			fread(memPage,sizeof(char),PAGE_SIZE,fileBuff);
			fHandle->curPagePos = pageNum;

			fclose(fileBuff);
			return RC_OK;
		}
	}
}

int getBlockPos (SM_FileHandle *fHandle){
	return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else{
		return readBlock(0,fHandle,memPage);
	}
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else{
		return readBlock(fHandle->curPagePos - 1,fHandle,memPage);
	}
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else{
		return readBlock(fHandle->curPagePos,fHandle,memPage);
	}
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else{
		return readBlock(fHandle->curPagePos + 1,fHandle,memPage);
	}
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	 if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else{
		return readBlock(fHandle->totalNumPages,fHandle,memPage);
	}
 }

 RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	 FILE *fileBuff;
	 size_t writeBlockSize;

	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	if(fHandle->mgmtInfo != NULL ){
		closePageFile(fHandle);// close the open file
	}
	fileBuff = fopen(fHandle->fileName,"r+");
	if(fileBuff == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else{
		if(fHandle->totalNumPages < pageNum || pageNum < 0){//the file has less than pageNum pages
			return RC_READ_NON_EXISTING_PAGE;
		}
		else{// read the pageNumth block from a file
			// seek to target page
			fseek(fileBuff,(pageNum + 1)*PAGE_SIZE,SEEK_SET);
			// store the content in the memory pointed to by the memPage page handle
			writeBlockSize = fwrite(memPage,sizeof(char),PAGE_SIZE,fileBuff);
			if(writeBlockSize != PAGE_SIZE){
				return RC_WRITE_FAILED;
			}
			//fHandle->curPagePos = pageNum + 1; updated at Feb 6th

			fclose(fileBuff);
			return RC_OK;
		}
	}
 }

 RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	 if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else{
		return writeBlock(fHandle->curPagePos,fHandle,memPage);
	}
 }

 RC appendEmptyBlock (SM_FileHandle *fHandle){
	 SM_PageHandle SM_P;
	 FILE *fileBuff;
	 size_t writeBlockSize;

	 SM_P = (char*)calloc(PAGE_SIZE, sizeof(char));

	 if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	 if(fHandle->mgmtInfo != NULL ){
		closePageFile(fHandle);// close the open file
	}
	fileBuff = fopen(fHandle->fileName,"r+");
	if(fileBuff == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else{
		fseek(fileBuff,(fHandle->totalNumPages + 1)*PAGE_SIZE,SEEK_SET); //point to the last page + 1
		writeBlockSize = fwrite(SM_P, sizeof(char), PAGE_SIZE, fileBuff);
		if(writeBlockSize != PAGE_SIZE){
			return RC_WRITE_FAILED;
		}
		fHandle->totalNumPages = fHandle->totalNumPages + 1;

		fclose(fileBuff);
		return RC_OK;
	}

 }

 RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	 int i;

	 if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	 else{
         int a = fHandle->totalNumPages;  //Updated on Feb 6th.
		 if(a < numberOfPages){
			 for(i = 0; i < (numberOfPages - a); i++){
				 appendEmptyBlock(fHandle);
			 }
			 return RC_OK;
		 }
		 return RC_OK;
	 }
 }
