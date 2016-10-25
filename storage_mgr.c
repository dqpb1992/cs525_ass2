//
//  storage_mgr.c
//  testcode_1_0919
//
//  Created by Pingyu Xue on 9/19/16.
//  Copyright © 2016 Pingyu Xue. All rights reserved.
//

#include "storage_mgr.h"
#include "stdio.h"
#include "string.h"
#include "stdlib.h"
#include "dberror.h"
/**************************************************************************************
 *Function Name: initStorageManager
 *
 *Description: Initialization of Storage Manager
 *
 *Parameters:
 *            Void
 *Return:
 *            Void
 *Author:
 *            Uday Tak <utak@hawk.iit.edu>
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/14/2016             Uday Tak <utak@hawk.iit.edu>        first create
 **************************************************************************************/
extern void initStorageManager (void)
{
    printf ("Initialization of the Storage Manager...\n");
}

/**************************************************************************************
 *Function Name: createPageFile
 *
 *Description:
 *          Create a new page file fileName. with initial file size of one page. Fill this single page with '\0' bytes.
 *  Steps -
 *              Create New File
 *          Allocate block of memory of PAGE_SIZE elements. Calloc initializes it's bits to zero
 *          Write the page to file
 *          Free the memory
 *          Close the file
 *Parameters:
 *              char *fileName
 *Return:
 *              RC_OK
 *Author:
 *              Uday Tak <utak@hawk.iit.edu>
 *Hisory:
 *              Date                  Name                                Content
 *              ----------            ------------------------------      ---------------
 *              9/14/2016             Uday Tak <utak@hawk.iit.edu>        first create
 *              9/20/2016             Pingyu Xue<pxue2@hawk.iit.edu>      rewrite
 **************************************************************************************/

// first create by Uday
//extern RC createPageFile (char *fileName)
//{
//    File* fptr;
//
//    /* open the file */
//    fptr = fopen(fileName, "rw+");
//
//    if (fptr == NULL)
//    {
//        return RC_FILE_OPEN_FAILED;
//    }
//
//    /* Allocate block of memory of PAGESIZE elements. Calloc initializes it's bits to zero */
//    SM_PageHandle blank_page = (SM_PageHandle) calloc(PAGESIZE,sizeof(char));
//
//    /* Write the page to file */
//    int FileWriteFlag = fwrite (blank_page, sizeof(char), PAGESIZE, fptr);
//
//    if(FileWriteFlag == 0)
//        return RC_FILE_WRITE_FAILED;
//
//    /* Free the memory */
//    free(blank_page);
//
//    /* Close the file */
//    int fileCloseFlag = fclose (fptr);
//
//    if (fileCloseFlag != 0)
//    {
//        printf("Error: File is closed with the error..\n");
//        return RC_FILE_CLOSE_FAILED;
//    }
//
//    return RC_OK;
//}

// Rewrite by Pingyu Xue
RC createPageFile(char *fileName){
    
    // pointer of the file, Memory size of the page.
    
    if (fileName == NULL){
        return RC_FILE_NOT_FOUND;
    }
    //Question(by Pingyu Xue):the file is create by ourself.  need check or not?
    //Answer(by Uday Tak): Needed. good programing practice
    
    FILE *newPageFile;
    newPageFile= fopen(fileName, "w+");
    
    if(newPageFile == NULL){
        fclose(newPageFile);
        return RC_CREATE_PAGE_FAIL;
    }
    
    //     If  create the page success ,then ,set the pointer of the page.
    //     do not waste the memeory! -- Uday : ok
    
    char *newPage;
    newPage = (char *) calloc(PAGE_SIZE, sizeof(char));
    
    //    write in and check the result.
    size_t writeCheck = 0;
    writeCheck = fwrite(newPage, sizeof(char), PAGE_SIZE, newPageFile);
    
    if (writeCheck == PAGE_SIZE){
        fclose(newPageFile);
        return RC_OK;
    }
    else{
        fclose(newPageFile);
        free(newPage);
        return RC_CREATE_PAGE_FAIL;}
    
    
}

// Question(by Pingyu Xue): "calloc()"  set a pointer is same with "memset( ,'\0',)"
//                           can we just switch the both?  ps.( see the test in main.c)
// Answer (by ) :



/**************************************************************************************
 *Function Name: openPageFile
 *
 *Description:
 *          Open the page file and Initialize the existing file handle
 *  Steps -
 *              Open the File
 *          Initialize the handle
 *          Initialize pointer to file's first position
 *Parameters:
 *              char *fileName,
 *      SM_FileHandle *fHandle
 *Return:
 *              RC_OK
 *Author:
 *              Uday Tak <utak@hawk.iit.edu>
 *Hisory:
 *              Date                  Name                                Content
 *              ----------            ------------------------------      ---------------
 *              9/15/2016             Uday Tak <utak@hawk.iit.edu>        first create
 *              9/20/2016             Pingyu Xue<pxue2@hawk.iit.edu>      rewrite
 **************************************************************************************/
// first create by Uday
//extern RC openPageFile (char *fileName, SM_FileHandle *fHandle)
//{
//    File* fptr;
//
//    /* open the file */
//    fptr = fopen(fileName, "rw+");
//
//    if (fptr == NULL)
//    {
//        return RC_FILE_OPEN_FAILED;
//    }
//
//    /*Get the file's end*/
//    int seekFlag = fseek( fptr, 0, SEEK_END );
//
//    if (seekFlag != 0)
//    {
//        return RC_SEEK_POSITION_FAILED;
//    }
//
//    int EndPosition = ftell(fptr);
//    if ( EndPosition == -1) {
//        return RC_TELL_POSITION_FAILED;
//    }
//
//    /*Get the total no of pages */
//    int TotalPages = (EndPosition + 1) / ;
//
//    /*Initialize pointer to file's first position*/
//    int seekFlag = fseek( fptr, 0, SEEK_SET);
//    if (seekFlag != 0)
//    {
//        return RC_SEEK_POSITION_FAILED;
//    }
//
//
//    //wrong code
//    /*Initialize the handle */
//    // here is the 1st version code:
//    fhandle -> fileName = filename;
//    fhandle -> totalNumPages = TotalPages;
//    fhandle -> curPagePos = 0;
//    fhandle -> mgmtInfo = fptr;
//
//    return RC_OK;

// Rewrite by Pingyu Xue:
RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    
    FILE *newFile;
    size_t pageSize = 1;
    int seekResult;
    
    newFile = fopen(fileName, "r");
    
    // open file right or not
    if( newFile != 0){
        seekResult = fseek(newFile, 0, SEEK_END);
        
        // seek set right or not
        if(seekResult == 0){
            pageSize = ftell(newFile);
            
            // indicator got or not
            if( pageSize != 0){
                
                size_t modPageNum = pageSize/PAGE_SIZE;
                /*(Question : Uday Tak) pageSize/pageSize will always return 1. so need to use some different logic.
                 (Answer : )
                 */
                
                // calculate the page number for the totalPageNum
                if (modPageNum) {
                    fHandle->totalNumPages =(int) modPageNum;    // test 3: size_t mod num
                }
                else{
                    fHandle->totalNumPages = (int) (modPageNum + 1);
                }
                
                // ininilize the info .
                fHandle->curPagePos = 0;
                fHandle->fileName = fileName;
                fHandle->mgmtInfo = newFile;
                
                // reset the pointer.
                fseek(newFile, 0, SEEK_SET);
                free(newFile);
                
                return RC_OK;
            }
            else
                //       free(newFile);
                return RC_FUNC_Ftell_ERROR;  // ftell error'
            
        }
        else
            //     free(newFile);
            return RC_FUNC_Fseek_ERROR;      // fseek error
    }
    else
        //    free(newFile);
        return RC_FILE_NOT_FOUND;
}


/**************************************************************************************
 *Function Name: closePageFile
 *
 *Description:
 *          Close the page file
 *  Steps -
 *              Close the File
 *          return if fails
 *Parameters:
 *      SM_FileHandle *fHandle
 *Return:
 *              RC_OK
 *Author:
 *              Uday Tak <utak@hawk.iit.edu>
 *Hisory:
 *              Date                  Name                                Content
 *              ----------            ------------------------------      ---------------
 *              9/17/2016             Uday Tak <utak@hawk.iit.edu>        first create
 *              9/20/2016             Pingyu Xue<pxue2@hawk.iit.edu>     add 'check fhandle' and fix 'return' request
 *              9/25/2016             Pingyu Xue<pxue2@hawk.iit.edu>      rewrite
 **************************************************************************************/


// RC closePageFile (SM_FileHandle *fHandle)
// {
//     if (fHandle == NULL){
//         return RC_FILE_HANDLE_NOT_INIT;
//     }
//    test flag 1:
//    printf("close test 1\n");

//  //   Note(Pingyu Xue)this fucntion is wrong
//    int fileCloseFlag = fclose(fHandle->mgmtInfo);

//    test flage 2:
//   printf("close test 2\n");


//     if (fileCloseFlag != 0)
//     {
//         return RC_FILE_CLOSE_ERROR;
//     }

//     return RC_OK;


//     // Question( by Pingyu Xue): should free memeory in this function ?
//     // Answer  (by Uday Tak): No. memory is not allocated at this step using calloc  so no need to free the memory.
// }


RC closePageFile (SM_FileHandle *fHandle)
{
    if (fHandle == NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    fHandle->totalNumPages = 0;
    
    fHandle->curPagePos = 0;
    
    fHandle->fileName = NULL;
    
    fHandle->mgmtInfo = NULL;
    
    
    return RC_OK;
    
    
    
    // Question( by Pingyu Xue): should free memeory in this function ?
    // Answer  (by Uday Tak): No. memory is not allocated at this step using calloc  so no need to free the memory.
}

/**************************************************************************************
 *Function Name: destroyPageFile
 *
 *Description:
 *          Destroy the page file
 *  Steps -
 *              Close the File
 *          return if fails
 *Parameters:
 *      SM_FileHandle *fHandle
 *Return:
 *              RC_OK
 *Author:
 *              Uday Tak <utak@hawk.iit.edu>
 *Hisory:
 *              Date                  Name                                Content
 *              ----------            ------------------------------      ---------------
 *              9/18/2016             Uday Tak <utak@hawk.iit.edu>        first create
 *              9/20/2016             Pingyu Xue<pxue2k@hawk.iit.edu>     rewrite
 **************************************************************************************/
//first create by Uday
//extern RC destroyPageFile (char *fileName)
//{
//    fileRemoveFlag = remove(fileName);
//   	if(fileRemoveFlag != 0)
//
//   	{
//      		return RC_FILE_REMOVE_FAILED;
//   	}
//    return RC_OK;
//}

// Rewrite by Pingyu Xue:
RC destroyPageFile (char *fileName)
{
    if (fileName == NULL){
        return RC_FILE_NOT_FOUND;
    }
    
    
    int removerFile = 1;
    /* (Question : Uday Tak) No need to assign any value to removeFile. if remove is successful it returns 0
     if it returns anything else apart from 0 that means operation was unsuccessful.
     (Answer :)
     */
    
    
    removerFile = remove(fileName);
    
    if (removerFile == 0) {
        return RC_OK;
    }
    else
        return RC_FILE_REMOVE_ERROR;
}

//Question(by Pingyu Xue):  remove ()? free memory ,should check again or not?
//Answer(by Uday TAk): No. reason is Same a as above






/**************************************************************************************
 *Function Name: readBlock
 *
 *Description:
 *            read any page(block) which the user want to read
 *            in a txt file which already has an SM_FileHandle
 *Parameters:
 *            int pageNum: page number of the page user want to read
 *            SM_FileHandle *fHandle: a strcture which saved opened txt file's Info
 *            SM_PageHandle memPage: a variable to save such page's content
 *Return:
 *            RC_OK;
 *Author:
 *            Pingyu Xue
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Pingyu Xue <pxue2@hawk.iit.edu>      first create
 *            9/21/2016             Pingyu Xue <pxue2@hawk.iit.edu>      add limit for the vars
 *            9/25/2016             Pingyu Xue <pxue2@hawk.iit.edu>      rewrite
 *            9/26/2016             Pingyu Xue <pxue2@hawk.iit.edu>      rewrite
 **************************************************************************************/


// RC readBlock(int pageNum, SM_FileHandle *fhandle, SM_PageHandle memPage)
// {
//     int seekpostion = 1;       // initialize the var   : (add by PyX　09/21)
//     size_t readStatement = 0;  // initialize the var   ：(add by PyX  09/21)

//     if (fhandle== NULL){
//         return RC_FILE_HANDLE_NOT_INIT;
//     }

//     if (memPage = NULL){
//     	return RC_mempage_null;
//     }

//     if (pageNum >= 0 && fhandle->totalNumPages > pageNum) { // if pageNum only >0 , readFirstPage() will be error! (add by PyX  9/21)
//         seekpostion=fseek(fhandle->mgmtInfo, sizeof(char)*PAGE_SIZE*pageNum, SEEK_SET);
//         // Q1:??? offset= sizeof(char)  || Do we need the sizeof(char)? Ans : (Uday) - yes. 1.good programing practice 2. instead of char in future u might use nchar
//         // Q4:??? why fandle->mamtInfo can be used for FILE *point? Ans:(Uday) - file pointer is different than filename

//         if (seekpostion == 0){
//             readStatement = fread(memPage, sizeof(char), PAGE_SIZE, fhandle->mgmtInfo);
//         // Q2:???  size_t sizeof(char)  ||
//             if (readStatement != 0){
//                 // if readStatment only ==  PAGE_SIZE, how about the file material less than a PAGE_SIZE?
//                 // Q5: ??? Should the read material be the size of PAGE_SIZE ?　    09/21 Ans:( Uday) : yes read material always of size page
//                 fhandle->curPagePos = pageNum;


//                 return RC_OK;
//             }
//         // Q3: how to check the fread() have the right result ? A:(uday) compare the length may be
//             else {
//                 return RC_READ_FAILED;
//             }
//         } else {
//             return RC_SET_POINTER_FAILED;   // channge to RC_FUNC_Fseek_ERROR    9/21
//         }
//     } else {
//         return RC_READ_NON_EXISTING_PAGE;
//     }
// }


// rewrite 9/25
RC readBlock(int pageNum, SM_FileHandle *fhandle, SM_PageHandle memPage)
{
    //   int seekpostion = 1;       // initialize the var   : (add by PyX　09/21)
    //   size_t readStatement = 0;  // initialize the var   ：(add by PyX  09/21)
    
    //   // if (fhandle== NULL){
    //   //     return RC_FILE_HANDLE_NOT_INIT;
    //   // }
    
    //   // delete becasue the test file
    //   // if (memPage = NULL){
    //   // 	return RC_mempage_null;
    //   // }
    
    //   FILE *fp;
    
    //   fp=fopen(fhandle->fileName,'r');
    
    //    // if (pageNum >= 0 && fhandle->totalNumPages > pageNum) { // if pageNum only >0 , readFirstPage() will be error! (add by PyX  9/21)
    // //       seekpostion=fseek(fp, sizeof(char)*PAGE_SIZE*pageNum, SEEK_SET);
    //          // seekpostion=fseek(fp, PAGE_SIZE*pageNum, SEEK_SET);
    //   int offset = PAGE_SIZE*pageNum;
    
    //         fseek(fp, offset, SEEK_SET);
    //       // Q1:??? offset= sizeof(char)  || Do we need the sizeof(char)? Ans : (Uday) - yes. 1.good programing practice 2. instead of char in future u might use nchar
    //       // Q4:??? why fandle->mamtInfo can be used for FILE *point? Ans:(Uday) - file pointer is different than filename
    
    //       // if (seekpostion == 0){
    //           // readStatement = fread(memPage, sizeof(char), PAGE_SIZE, fp);
    //        fread(memPage, sizeof(char), PAGE_SIZE, fp);
    //       // Q2:???  size_t sizeof(char)  ||
    //           // if (readStatement != 0){
    //               // if readStatment only ==  PAGE_SIZE, how about the file material less than a PAGE_SIZE?
    //               // Q5: ??? Should the read material be the size of PAGE_SIZE ?　    09/21 Ans:( Uday) : yes read material always of size page
    //               fhandle->curPagePos = pageNum;
    
    //               fclose(fp);
    
    //               return RC_OK;
    //   //       }
    
    //   //     Q3: how to check the fread() have the right result ? A:(uday) compare the length may be
    //   //         else {
    //   //             return RC_READ_FAILED;
    //   //         }
    //   //     } else {
    //   //         return RC_SET_POINTER_FAILED;   // channge to RC_FUNC_Fseek_ERROR    9/21
    //   //     }
    //   // } else {
    //   //     return RC_READ_NON_EXISTING_PAGE;
    //   // }
    
    //   if(pageNum>fhandle->totalNumPages-1||pageNum<0)
    //        return RC_READ_NON_EXISTING_PAGE;
    
    
    // rewite  9/26
    //  int seekpostion ;
    //  size_t readStatement ;
    
    FILE *fp;
    
    fp=fopen(fhandle->fileName,"r");
    
    if (fhandle== NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    if (memPage == NULL){
        return RC_mempage_null;
    }
    
    if (pageNum < 0 && fhandle->totalNumPages < pageNum){
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    
    
    if (fseek(fp,fhandle->curPagePos*PAGE_SIZE,SEEK_SET) == 0){
        
        if(fread(memPage,sizeof(char),PAGE_SIZE,fp)){
            fhandle->curPagePos=pageNum;
            fclose(fp);
            
            return RC_OK;
            
        }
    }
    return RC_FUNC_Fseek_ERROR;
    
}

/**************************************************************************************
 *Function Name: getBlockPos
 *
 *Description:
 *            find which page is reading
 *Parameters:
 *            SM_FileHandle *fHandle: a strcture which saved opened txt file's Info
 *Return:
 *            (int) page number reading right now;
 *Author:
 *            Pingyu Xue
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Pingyu Xue <pxue2@hawk.iit.edu>      first create
 **************************************************************************************/

extern int getBlockPos(SM_FileHandle *fhandle){
    if (fhandle != NULL) {
        return fhandle->curPagePos;
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

/**************************************************************************************
 *Function Name: readFirstBlock
 *
 *Description:
 *            show first page content onto screen
 *            update the fHandle info
 *Parameters:
 *            SM_FileHandle *fHandle: a strcture which saved opened txt file's Info
 *            SM_PageHandle memPage: a variable to save such page's content
 *Return:
 *            RC_OK
 *Author:
 *            Pingyu Xue
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Pingyu Xue <pxue2@hawk.iit.edu>      first create
 **************************************************************************************/

RC readFirstBlock(SM_FileHandle *fhandle, SM_PageHandle mempage)
{
    
    if (fhandle != NULL){
        
        return readBlock(0, fhandle, mempage);
    }
    else
        return RC_FILE_HANDLE_NOT_INIT;
    
    
    // return readBlock(0,fhandle,mempage);
}

/**************************************************************************************
 *Function Name: readPreviousBlock
 *
 *Description:
 *            show previous page content onto screen
 *            update the fHandle info
 *Parameters:
 *            SM_FileHandle *fHandle: a strcture which saved opened txt file's Info
 *            SM_PageHandle memPage: a variable to save such page's content
 *Return:
 *            RC_OK
 *Author:
 *            Pingyu Xue
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Pingyu Xue <pxue2@hawk.iit.edu>      first create
 **************************************************************************************/

RC readPreviousBlock(SM_FileHandle *fhandle, SM_PageHandle mempage)
{
    // Does it have previous block?
    if (fhandle != NULL) {
        if (fhandle->curPagePos != 0) {
            int currentPage= (fhandle->curPagePos)-1;
            return readBlock(currentPage, fhandle, mempage);
        }
        else
            return RC_READ_NON_EXISTING_PAGE;
    }
    else
        return RC_FILE_HANDLE_NOT_INIT;
}

/**************************************************************************************
 *Function Name: readCurrentBlock
 *
 *Description:
 *            show previous page content onto screen
 *Parameters:
 *            SM_FileHandle *fHandle: a strcture which saved opened txt file's Info
 *            SM_PageHandle memPage: a variable to save such page's content
 *Return:
 *            RC_OK
 *Author:
 *            Pingyu Xue
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Pingyu Xue <pxue2@hawk.iit.edu>      first create
 **************************************************************************************/

RC readCurrentBlock(SM_FileHandle *fhandle, SM_PageHandle mempage)
{
    if (fhandle != NULL)
    {
        int currentPage = fhandle->curPagePos;
        return readBlock(currentPage, fhandle, mempage);
    }
    else
        return RC_FILE_HANDLE_NOT_INIT;
}

/**************************************************************************************
 *Function Name: readNextBlock
 *
 *Description:
 *            show next page content onto screen
 *            update the fHandle info
 *Parameters:
 *            SM_FileHandle *fHandle: a strcture which saved opened txt file's Info
 *            SM_PageHandle memPage: a variable to save such page's content
 *Return:
 *            RC_OK
 *Author:
 *            Pingyu Xue
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Pingyu Xue <pxue2@hawk.iit.edu>      first create
 **************************************************************************************/

RC readNextBlock(SM_FileHandle *fhandle, SM_PageHandle mempage)
{
    //    Does it have next block?
    if (fhandle != NULL){
        if(fhandle->curPagePos < fhandle->totalNumPages) {
            int currentPage = (fhandle->curPagePos) + 1;
            return readBlock(currentPage, fhandle, mempage);
        }
        else
            return RC_READ_NON_EXISTING_PAGE;
    }
    else
        return RC_FILE_HANDLE_NOT_INIT;
}

/**************************************************************************************
 *Function Name: readLastBlock
 *
 *Description:
 *            show last page content onto screen
 *            update the fHandle info
 *Parameters:
 *            SM_FileHandle *fHandle: a strcture which saved opened txt file's Info
 *            SM_PageHandle memPage: a variable to save such page's content
 *Return:
 *            RC_OK
 *Author:
 *            Pingyu Xue
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Pingyu Xue <pxue2@hawk.iit.edu>      first create
 *            9/19/2016             Hanqiao Lu <hlu22@hawk.iit.edu>    add edge case, improve coding style
 **************************************************************************************/


RC readLastBlock(SM_FileHandle *fhandle, SM_PageHandle mempage){
    if (fhandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if (mempage == NULL) {
        return RC_NO_SUCH_PAGE_IN_BUFF;
    }
    return readBlock(fhandle->totalNumPages, fhandle, mempage);
}

/**************************************************************************************
 *Function Name: writeBlock
 *
 *Description: Write a page to disk using absolute position.
 *
 *
 *Parameters:
 *            int pageNum  The number of page user want to write
 *            SM_FileHandle *fHandle: A pointer to a file structure contains metadata about file
 *            SM_PageHandle memPage: the location where the block will be stored in memory
 *Return:
 *            RC_OK
 *Author:
 *            Hanqiao Lu
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Hanqiao Lu <hlu22@hawk.iit.edu>      first create
 *            9/25/2016             Pingyu Xue <pxue2@hawk.iit.edu>      test case fixing
 *            9/26/2016             Pingyu Xue <pxue2@hawk.iit.edu>      fix function error and flaws
 **************************************************************************************/


RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int setSuccess;
    size_t writeState;
    
    // add the FILE pointer
    FILE * fp;
    fp = fopen(fHandle->fileName, "r+");
    
    //test case :
    // if (fHandle == NULL) {
    //     return RC_FILE_HANDLE_NOT_INIT;
    // }
    
    // if (memPage ==  NULL) {
    //     return RC_NO_SUCH_PAGE_IN_BUFF;
    // }
    
    
    //    // change " pageNum <= 0 " to  "pageNum< 0 "   Pingyu Xue 9/25
    //    if (pageNum < 0 || fHandle->totalNumPages < pageNum) {
    //        return RC_READ_NON_EXISTING_PAGE;
    //    }
    
    // fix fHandle-> mgmInfo   to  fp  pingyuXue 0926
    setSuccess = fseek(fp, sizeof(char) * PAGE_SIZE * pageNum, SEEK_SET);
    if (setSuccess != 0) {
        return RC_SET_POINTER_FAILED;
    }
    
    //    // fix fHandle-> mgmInfo to fp; Pingyu Xue  0926
    writeState = fwrite(memPage, sizeof(char), PAGE_SIZE, fp);
    
    // change  "writeState != PAGE_SIZE" to "writeState =533= 0" Pingyu Xue 9/25
    if (writeState == 0) {
        return RC_WRITE_FAILED;
    }
    
    // fclose(fp);
    
    
    //    return RC_OK;
    
    //    FILE *fp;
    //    RC rv;
    
    // fp=fopen(fHandle->fileName,"rb+");
    // if(fseek(fp,pageNum * PAGE_SIZE, SEEK_SET) != 0){
    // 	rv = RC_READ_NON_EXISTING_PAGE;
    // } else if (fwrite(memPage, PAGE_SIZE, 1, fp) != 1){
    // 	rv = RC_WRITE_FAILED;
    // } else {
    // 	fHandle->curPagePos=pageNum;		//Success write block, then curPagePos should be changed.
    // 	rv = RC_OK;
    // }
    
    fclose(fp);
    
    return RC_OK;
}


/**************************************************************************************
 *Function Name: writeBlock
 *
 *Description: Write the current page to disk using absolute position.
 *
 *
 *Parameters:
 *            SM_FileHandle *fHandle: A pointer to a file structure contains metadata about file
 *            SM_PageHandle memPage: the location where the block will be stored in memory
 *Return:
 *            RC_OK
 *Author:
 *            Hanqiao Lu
 *Hisory:
 *            Date                  Name                                Content
 *            ----------            ------------------------------      ---------------
 *            9/19/2016             Hanqiao Lu <hlu22@hawk.iit.edu>      first create
 **************************************************************************************/
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if (memPage == NULL) {
        return RC_NO_SUCH_PAGE_IN_BUFF;
    }
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}



/**************************************************************************************
 *Function Name: appendEmptyBlock
 *
 *Description: Increase the number of pages in the file by one by adding an empty page at the end of file and filling it with zero bytes.
 *
 *Parameters:
 *            SM_FileHandle *fHandle: A pointer to a file structure containing metadata about file
 *Return:
 *            retstat - variable of type RC which contains return values obtained from checking conditions such as file write successful or not.
 *Author:
 *            Vaibhav Uday Hongal
 *History:
 *            Date                  Name                                              Content
 *            ----------            -----------------------------------------         ---------------
 *            9/19/2016             Vaibhav Uday Hongal <vhongal@hawk.iit.edu>        first create
 *
 **************************************************************************************/
RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    RC retstat;
    int fileseekstatus, filewritestatus;
    
    SM_PageHandle blankpage = (SM_PageHandle *)calloc(PAGE_SIZE, sizeof(char));
    fileseekstatus = fseek(fHandle->mgmtInfo, (fHandle->totalNumPages + 1)*PAGE_SIZE*sizeof(char), SEEK_SET);
    if(fileseekstatus != 0)
    {
        retstat = RC_SET_POINTER_FAILED;
    }
    else
    {
        filewritestatus = fwrite(blankpage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
        if(filewritestatus != PAGE_SIZE)
            retstat = RC_WRITE_FAILED;
        else
            fHandle->totalNumPages += 1;
        fHandle->curPagePos = fHandle->totalNumPages;
        retstat = RC_OK;
    }
    free(blankpage);
    return retstat;
}



/**************************************************************************************
 *Function Name: ensureCapacity
 *
 *Description: If the file has less than numberOfPages pages then increase them to numberOfPages.
 *
 *Parameters:
 *            int numberOfPages: The input value to check the number of pages in file and ensure its set to numberOfPages.
 *            SM_FileHandle *fHandle: A pointer to a file structure containing metadata about file
 *Return:
 *            retstat - variable of type RC which contains return values obtained from checking conditions such as file write successful or not
 or RC_OK if everything is okay.
 *Author:
 *            Vaibhav Uday Hongal
 *History:
 *            Date                  Name                                              Content
 *            ----------            -----------------------------------------         ---------------
 *            9/19/2016             Vaibhav Uday Hongal <vhongal@hawk.iit.edu>        first create
 *
 **************************************************************************************/
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    RC retstat;
    int i, n = 0;
    if(fHandle->totalNumPages < numberOfPages)
    {
        n = numberOfPages - fHandle->totalNumPages;
        for (i=0; i<n; i++)
            retstat = appendEmptyBlock(fHandle);
    }
    if(retstat != RC_OK)
    {
        return retstat;
    }
    return RC_OK;
}




/************************************END OF FUNCTIONS********************************/


