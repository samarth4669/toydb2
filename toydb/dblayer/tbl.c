
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

int  getLen(int slot, byte *pageBuf);
int  getNumSlots(byte *pageBuf);
void setNumSlots(byte *pageBuf, int nslots);
int  getNthSlotOffset(int slot, char* pageBuf);


/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    //Added Code//
    // Initialize PF, create PF file,
    PF_Init();
    if(overwrite){
        PF_DestroyFile(dbname);
    }
    // allocate Table structure  and initialize and return via ptable
    int error;
    int fp;
    int pagenumber;
    Table *tbpointer;
    char *dbpagebuffer;
    fp = PF_OpenFile(dbname);
    if(fp<0){
        error = PF_CreateFile(dbname);
        checkerr(error);
        fp = PF_OpenFile(dbname);
        checkerr(fp);
    }
    tbpointer = (Table *)malloc(sizeof(Table));
    tbpointer->schema = schema;
    tbpointer->fd = fp;

    error = PF_GetFirstPage(fp, &pagenumber, &dbpagebuffer);
    if(error==PFE_OK){
        tbpointer->firstpage = -1;
        tbpointer->currentpage =-1;
        tbpointer->pagebuf = NULL;
        PF_UnfixPage(fp, pagenumber, false);
    }
    else if(error==PFE_EOF){
        tbpointer->firstpage = pagenumber;
        tbpointer->currentpage = pagenumber;
        tbpointer->pagebuf = NULL;
    }

    else{
        PF_PrintError("TABLE-INSERT: PF_GetFirstPage");
        PF_CloseFile(fp);
        free(tbpointer);
        return error;
    }
    *ptable = tbpointer;
    return 0;
}

void
Table_Close(Table *tbl) {
    //Added Code//
    // Unfix any dirty pages, close file.
    if(tbl==NULL){
        return;
    }
    int error = PF_CloseFile(tbl->fd);
    checkerr(error);
    free(tbl);

    //********************************************* */
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {
    //Added Code//
    int error;
    if(tbl->currentpage==-1 && tbl->firstpage==-1 && tbl->pagebuf==NULL){
        error = Allocate_Page(tbl);
        checkerr(error);
        tbl->firstpage = tbl->currentpage;

    }
    else{
        error = searchspace(tbl, len);
        if(error==-1||error==PFE_EOF){
            error = Allocate_Page(tbl);
        }
    }
    copytospace(tbl, record, len, rid);
    return 0;
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    //*************************************************************** */
}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;
    int len;
    char *pagebuffer;
    pageheader *header;
    //Added Code//
    int error;
    error = PF_GetThisPage(tbl->fd, pageNum, &pagebuffer);
    if(error!=PFE_OK && error!=PFE_PAGEFIXED){
        PF_PrintError("TABLE_GET ");
        return 0;
    }
    header = (pageheader *)pagebuffer;
    int offset = header->recordlocation[slot];
    int record_size = inslot_record_size(header, slot);
    if (record_size > maxlen)
        memcpy(record, &pagebuffer[offset], maxlen);
    else
        memcpy(record, &pagebuffer[offset], record_size);
    PF_UnfixPage(tbl->fd, pageNum, false);
    return record_size;


    //****************************************************** */
    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    //Added_code//
    int pagenumber = -1, error, rec_len;
    char *pagebuffer;
    RecId recID;
    byte record[INPAGE_MAXPOSS_RECORD_SIZE];
    pageheader *header;
    // For each page obtained using PF_GetNextPage
    while (1)
    {
        error = PF_GetNextPage(tbl->fd, &pagenumber, &pagebuffer);
        if (error == PFE_EOF) // Stop if there are no more pages in table
            break;
        if (error == PFE_OK)
        {
            header = (pageheader*)pagebuffer;
            // Unfix the page since Table_Get will be calling pf_getthispage
            PF_UnfixPage(tbl->fd, pagenumber, false);
            //    for each record in that page,
            for (int i = 0; i < header->totalrecords; i++)
            {
                recID = createrecordid(pagenumber, i);
                rec_len = Table_Get(tbl, recID, record, INPAGE_MAXPOSS_RECORD_SIZE);
                callbackfn(callbackObj, recID, record, rec_len);
            }
        }
        else
        {
            PF_PrintError("TABLE_SCAN");
            break;
        }
    }





    //****************************************** */
}



//Added Code

int Allocate_Page(Table *table)
{
    int pagenumber;
    char *pagebuffer;
    // Allocate a new page
    int error = PF_AllocPage(table->fd, &pagenumber, &pagebuffer);
    checkerr(error);
    // Set up page header
    pageheader *header = (pageheader*)pagebuffer;

    // Set the initial number of slots to 0
    header->totalrecords = 0;

    // Set offset to the end of free space region by pointing at last byte
    header->freespaceend = PF_PAGE_SIZE - 1;

    // Update table structure
    table->currentpage = pagenumber;
    table->pagebuf = pagebuffer;
    return 0;
}

int copytospace(Table *table, byte *record, int len, RecId *rid)
{
    pageheader *header = (pageheader*) table->pagebuf;
    memcpy(inpage_insert_region(header, table->pagebuf, len), record, len);
    int slot = header->totalrecords; // Get next inpage empty record slot
    header->recordlocation[slot] = header->freespaceend - len + 1; // Adds the offset to this record in slot
    header->totalrecords += 1; // Added a new record
    header->freespaceend -= len; // Freespace shrinks by size of record in bytes
    // Unfix the page
    int error = PF_UnfixPage(table->fd, table->currentpage, TRUE);
    checkerr(error);
    // Set record id
    *rid = createrecordid(table->currentpage, slot);
    return 0;
}

int searchspace(Table *table, int len)
{
    if (table == NULL || len <= 0)
    {
        return -1; // Invalid input
    }

    char *pagebuffer = NULL;
    int pagenumber = table->currentpage;
    pageheader *header;
    int error = PF_GetThisPage(table->fd, pagenumber, &pagebuffer);

    // Iterate through all pages in the table
    while (1)
    {
        checkerr(error);

        // Access the page header from the page buffer
        header = (pageheader*)pagebuffer;

        int freesize = page_free_space_left(header) - pageheader_attr_size;
        if (freesize>= len)
        {
            table->pagebuf = pagebuffer;
            table->currentpage = pagenumber; 
            return pagenumber; 
        }
        // Unfix this page
        PF_UnfixPage(table->fd, pagenumber, FALSE);
        // Move to the next page
        error = PF_GetNextPage(table->fd, &pagenumber, &pagebuffer);

        if (error == PFE_EOF)
            break; // No more pages
    }

    return -1;
}


