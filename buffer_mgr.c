//
//  buffer_mgr.c
//  testcode_1_1010
//
//  Created by PingyuXue on 10/12/16.
//  Copyright © 2016 pingyuXue. All rights reserved.
//

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "stdio.h"
#include "stdlib.h"
#include "dt.h"

// cannot use for the right now
#include "replacement_strategy.h"



// Structure for the Bufferpool and pageFrame
/***************************************************************
 * Function Name: Create Structrue
 *
 * Description: Create structure
 *
 * Parameters: NULL
 *
 * Return: NULL
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/25      Pingyu XUe                     Create
 ***************************************************************/
typedef struct Bufferpool{
    BM_BufferPool bufferPool_bm;
    int num_Read_IO;
    int num_Write_IO;
    
}BufferPool;

typedef struct pageFrame{
    BM_PageHandle pageHandle;
    bool dirty;
    int fixCounts;
    
    void *mgmTransferData;  // for the information transfer into mgmtDat
}pageFrame;


// Buffer Manager Interface Pool Handling
/***************************************************************
 * Function Name: initBufferPool
 *
 * Description: initialize the BufferPool
 *
 * Parameters: 
 *           BM_BufferPool *const bm,
 *           const char *const pageFileName,
 *           const int numPages,
 *           ReplacementStrategy strategy,
 *           void *stratData.
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/25      Pingyu XUe                     Create
 ***************************************************************/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){
    
    BufferPool BM;
    pageFrame *pf = (pageFrame *)calloc(numPages,sizeof(pageFrame));
    
    FILE *file;
    
    
    file = fopen(pageFileName, "r");
    
    if (file != NULL){
        BM.bufferPool_bm.pageFile = (char *) pageFileName;
        BM.bufferPool_bm.numPages = numPages;
        BM.bufferPool_bm.strategy = strategy;
        BM.bufferPool_bm.mgmtData = pf;
        BM.bufferPool_bm.mgmtData->mgmTransferData = stratData;
        
        for ( int i = 0; i< BM.bufferPool_bm.numPages; i++){
            (BM.bufferPool_bm.mgmtData +i)->dirty = 0;
            (BM.bufferPool_bm.mgmtData +i)->fixCounts =0;
            (BM.bufferPool_bm.mgmtData +i)->pageHandle.pageNum= -1;
            (BM.bufferPool_bm.mgmtData +i)->pageHandle.data = NULL;
        }
        
        BM.num_Read_IO = 0;
        BM.num_Write_IO = 0;
        
        fclose(file);
        return RC_OK;
    }
    else
        return RC_FILE_NOT_FOUND;
}

/***************************************************************
 * Function Name: shutdownBufferPool
 *
 * Description: shut down the BufferPool
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/25      Pingyu XUe                     Create
 ***************************************************************/
RC shutdownBufferPool(BM_BufferPool *const bm){
    
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    
    pageFrame *pf = (pageFrame *)calloc(BM.bufferPool_bm.numPages,sizeof(pageFrame));
    
    // check the buffer can be shut down or not.
    int *fixcount = getFixCounts(bm);
    for (int i=0; i<BM.bufferPool_bm.numPages; ++i){
        if (* (fixcount +i)){
            free(fixcount);
            return RC_SHUT_DOWN_POOL_ERROR;
        }
    }
    
    // write the data into disk
    
    
    // I want to write the data into disk.
    for (int i=0; i<BM.bufferPool_bm.numPages; i++){
        (pf +i)->pageHandle.pageNum = 0;
        free((pf +i)->pageHandle.data);
        (pf +i)->fixCounts = 0;
        free((pf +i)->mgmTransferData);
    }
    
    free(BM.bufferPool_bm.pageFile);
    BM.bufferPool_bm.strategy = -1;
    BM.bufferPool_bm.numPages = 0;
    
    // alredy free all the data in pf before.
    free(BM.bufferPool_bm.mgmtData);
    
    free(pf);
    
    return RC_OK;
}

/***************************************************************
 * Function Name: forceFlushPool
 *
 * Description:
 *
 * Parameters:
 *
 * Return:
 *
 * Author:
 *
 * History:
 ***************************************************************/
RC forceFlushPool(BM_BufferPool *const bm);


// Buffer Manager Interface Access Pages
/***************************************************************
 * Function Name: markDirty
 *
 * Description: mark a page as dirty page
 *
 * Parameters: 
  *          BM_BufferPool *const bm,
 *           BM_PageHandle *const page
 *
 * Return:RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/12       Pingyu XUe                    Create
 ***************************************************************/
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    
    // the pf here should add a new memeory ?(Q1: PingyuXue 10/25/2016)
//    
//    pageFrame *pf = (pageFrame* )calloc( BM.bufferPool_bm.numPages, sizeof(pageFrame));
    
    pageFrame *pf;
    pf->pageHandle = *page;
    
    int count = 0;
    
    while (count++ < BM.bufferPool_bm.numPages){
        if((BM.bufferPool_bm.mgmtData +count)->pageHandle.pageNum == page->pageNum){
            // is that both pf-> and BM.bufferPoll_bm.mgmData->data should be changed?
            pf->dirty = true;
            BM.bufferPool_bm.mgmtData->dirty= true;
            
            return RC_OK;
        }
//        count++;
    }
    
    // did not find the value
    return RC_UNESPECTED_ERROR;
}

/***************************************************************
 * Function Name: unpinPage
 *
 * Description:unpin a page
 *
 * Parameters:
 *           BM_BufferPool *const bm
 *           BM_PageHandle *const page
 *
 * Return:RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/25      Pingyu XUe                     Create
 ***************************************************************/
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    
    pageFrame *pf;
    pf->pageHandle = *page;
    
    int count =0;
    
    while (count++ < BM.bufferPool_bm.numPages){
        if( (BM.bufferPool_bm.mgmtData + count)->pageHandle.pageNum == page->pageNum){
            
            if ((BM.bufferPool_bm.mgmtData + count)->fixCounts > 1){
                (BM.bufferPool_bm.mgmtData +count)->fixCounts--;
                return RC_OK;
            }
            else
                return RC_UNESPECTED_ERROR;
        }
        
    }
    
    // did not find the value
    return RC_UNESPECTED_ERROR;
}


/***************************************************************
 * Function Name: forcePage
 *
 * Description: write the current content of the page back to the page file on disk
 *
 * Parameters:
 *            BM_BufferPool *const bm
 *            BM_PageHandle *const page
 *
 * Return:RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/12      Pingyu XUe                     Create
 ***************************************************************/
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    
    pageFrame *pf;
    pf->pageHandle = *page;
    
    FILE *file;
    
    // step1: open file and find the locate of the file
    file = fopen(BM.bufferPool_bm.pageFile, "rb+");
    
    if( file != NULL){
        fseek(file, (page->pageNum)*PAGE_SIZE, SEEK_SET);
        fwrite(page->data, PAGE_SIZE, 1, file);
        BM.num_Write_IO++;
        fclose(file);
    }
    
    // step2: make the dirty clean
    int count = 0;
    
    while (count++ < BM.bufferPool_bm.numPages){
        if ((BM.bufferPool_bm.mgmtData + count)->pageHandle.pageNum == page->pageNum){
            (BM.bufferPool_bm.mgmtData + count)->dirty = false;
            pf->dirty = false;
            return RC_OK;
        }
    }
    
    // did not find the value
    return RC_UNESPECTED_ERROR;

}

/***************************************************************
 * Function Name: 
 *
 * Description:
 *
 * Parameters:
 *
 * Return:
 *
 * Author:
 *
 * History:
 ***************************************************************/
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum);



// Statistics Interface
/***************************************************************
 * Function Name: getFrameContents
 *
 * Description: Returns an array of PageNumbers where the ith element is the number of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: PageNumber *
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/12      Pingyu Xue                     Create
 *   2016/10/25      Pingyu Xue                     re-write
 ***************************************************************/
PageNumber *getFrameContents (BM_BufferPool *const bm){
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    pageFrame *pf = BM.bufferPool_bm.mgmtData;
    
    PageNumber *frameContents = (PageNumber*)malloc(BM.bufferPool_bm.numPages*sizeof(PageNumber));
    
    for (int i=0; i<BM.bufferPool_bm.numPages; i++){
        if((pf+i)->pageHandle.data == NULL){
            frameContents[i] = NO_PAGE;
        }
        else{
            frameContents[i] = (pf+i)->pageHandle.pageNum;
        }
    }
    
    return frameContents;
}

/***************************************************************
 * Function Name: getDirtyFlags
 *
 * Description: Returns an array of bools where the ith element is TRUE if the page stored in the ith page frame is dirty
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: bool 
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/12      Pingyu XUe                     Create
 *   2016/10/25      Pingyu Xue                     re-write
 ***************************************************************/
bool *getDirtyFlags (BM_BufferPool *const bm){
    
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    
    pageFrame *pf = BM.bufferPool_bm.mgmtData;

    bool *dirtyflag = (bool*) malloc (BM.bufferPool_bm.numPages * sizeof(bool));
    
    for (int i=0; i< BM.bufferPool_bm.numPages; i++){
        dirtyflag[i]= (pf + i)->dirty;
    }
    
    
    return dirtyflag;
}

/***************************************************************
 * Function Name: getFixCounts
 *
 * Description: Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: int *
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/12      Pingyu XUe                     Create
 *   2016/10/25      Pingyu XUe                     re-write
 ***************************************************************/
int *getFixCounts (BM_BufferPool *const bm){
    
    BufferPool BM;
    BM.bufferPool_bm = *bm ;
    
    pageFrame *pf = BM.bufferPool_bm.mgmtData;
    
    // set the memory for the fixcount
    int *FixCount = (int*) malloc (BM.bufferPool_bm.numPages * sizeof( int));
    
    for (int i=0; i< BM.bufferPool_bm.numPages;i++){
        FixCount[i] = (pf+ i)->fixCounts;
    }
    
    return FixCount;
}

/***************************************************************
 * Function Name: getNumReadIO
 *
 * Description:  Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: int
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/12      Pingyu XUe                     Create
 *   2016/10/25      Pingyu XUe                     re-write
 ***************************************************************/
int getNumReadIO (BM_BufferPool *const bm){
    
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    
    return BM.num_Read_IO;
}

/***************************************************************
 * Function Name: getNumWriteIO
 *
 * Description: Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: int
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   2016/10/12      Pingyu Xue                     Create
 *   2016/10/25      Pingyu Xue                     re-write
 ***************************************************************/
int getNumWriteIO (BM_BufferPool *const bm){
    
    BufferPool BM;
    BM.bufferPool_bm = *bm;
    
    return BM.num_Write_IO;
}


/************************************END OF FUNCTIONS********************************/

