#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>

#define VARCHAR 1
#define INT     2
#define LONG    3
// NEW defines


//#define pageheader_fixed_count (2)
//#define pageheader_variable_count (header->totalrecords)
#define pageheader_attr_size (sizeof(short))
#define pageheader_size(header) ( (header->totalrecords +2) *pageheader_attr_size) 

#define page_free_space_left(header) (header->freespaceend - pageheader_size(header) + 1) 
#define INPAGE_MAXPOSS_RECORD_SIZE (PF_PAGE_SIZE - 3*pageheader_attr_size) // Three attributes in page header
#define inpage_insert_region(header,pagebuffer,length) ( (pagebuffer + header->freespaceend) - length + 1) 

#define createrecordid(pagenum,slot) (pagenum << 16 | slot) // 4 Byte Rec ID : [ (MSB) 2 Byte Page num | 2 Byte slot num inside that page (LSB) ]
#define inslot_record_size(header,slot) ( (slot == 0) ?(PF_PAGE_SIZE - header->recordlocation[0]) : (header->recordlocation[slot-1] - header->recordlocation[slot]) ); 








//******************************************* *//
typedef char byte;

typedef struct {
    char *name;
    int  type;  // one of VARCHAR, INT, LONG
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc **columns; // array of column descriptors
} Schema;

//Added Code
typedef struct{
    short totalrecords;
    short freespaceend;

    short recordlocation[];
} pageheader;

    typedef struct
{
    Schema *schema;
   //Added Code
    int fd;
    int firstpage;
    int currentpage;
    char *pagebuf;

} Table;

typedef int RecId;

//ADDED Code
int Allocate_Page(Table *table);
int searchspace(Table *table, int len);
int copytospace(Table *table, byte *record, int len, RecId *rid);
//************************************** *//
int
Table_Open(char *fname, Schema *schema, bool overwrite, Table **table);

int
Table_Insert(Table *t, byte *record, int len, RecId *rid);

int
Table_Get(Table *t, RecId rid, byte *record, int maxlen);

void
Table_Close(Table *);

typedef void (*ReadFunc)(void *callbackObj, RecId rid, byte *row, int len);

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn);

#endif
