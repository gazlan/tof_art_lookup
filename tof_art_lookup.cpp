/* ******************************************************************** **
** @@ tof_art_lookup
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  : All necessary include files (tci.h) and import library (tci.lib)
** @  Notes  : are installed with TCI SDK package into corresponding subdirectories.
** ******************************************************************** */

/* ******************************************************************** **
**                uses pre-compiled headers
** ******************************************************************** */

/* ******************************************************************** **
**                uses pre-compiled headers
** ******************************************************************** */

#include "stdafx.h"

#include <stdlib.h>
#include <stdio.h>

#include "tci.h"
#include "tberror.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

#ifdef NDEBUG
#pragma optimize("gsy",on)
#pragma comment(linker,"/FILEALIGN:512 /MERGE:.rdata=.text /MERGE:.data=.text /SECTION:.text,EWR /IGNORE:4078")
#endif

/* ******************************************************************** **
** @@                   internal defines
** ******************************************************************** */

#define TD_DB_NAME                  "TECDOC_CD_3_2015@localhost:2024"
#define TD_TB_LOGIN                 "tecdoc"
#define TD_TB_PASSWORD              "tcd_error_0"
#define TD_MAX_SEGMEBT_RECORDS_ID   (250000)

struct TOF_ART_LOOKUP
{
   Int4           _iARL_ART_ID;           //  INtEGER               
   char*          _pszARL_SEARCH_NUMBER;  //  VARCHAR(105)
   char           _cARL_KIND;             //  CHAR(1)
   Bits           _biARL_CTM;             //  BITS(*)
   Int2           _iARL_BRA_ID;           //  SMALLINT
   char*          _pszARL_DISPLAY_NR;     //  VARCHAR(105)
   Int2           _iARL_DISPLAY;          //  SMALLINT
   Int2           _iARL_BLOCK;            //  SMALLINT
   Int2           _iARL_SORT;             //  SMALLINT
};

/* ******************************************************************** **
** @@                   internal prototypes
** ******************************************************************** */

/* ******************************************************************** **
** @@                   external global variables
** ******************************************************************** */

extern DWORD      dwKeepError = 0;

/* ******************************************************************** **
** @@                   static global variables
** ******************************************************************** */
                           
static TCIEnvironment*        pEnv  = NULL;
static TCIError*              pErr  = NULL;
static TCIConnection*         pConn = NULL;
static TCIStatement*          pStmt = NULL;
static TCIResultSet*          pRes  = NULL;
static TCITransaction*        pTa   = NULL;

static const char*   _pszTable = "tof_art_lookup";

static FILE*         _pOut = NULL;

/* ******************************************************************** **
** @@                   real code
** ******************************************************************** */

/* ******************************************************************** **
** @@ AllocationError()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void AllocationError(char* what)
{
   printf("Can't allocate %s\n",what);

   if (pEnv)
   {
      TCIFreeEnvironment(pEnv);
   }

   exit(1);
}

/* ******************************************************************** **
** @@ Init()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void Init()
{
   if (TCIAllocEnvironment(&pEnv))
   {
      AllocationError("environment handle");
   }

   if (TCIAllocError(pEnv,&pErr))
   {
      AllocationError("error handle");
   }

   if (TCIAllocTransaction(pEnv,pErr,&pTa))
   {
      AllocationError("transaction handle");
   }

   if (TCIAllocConnection(pEnv,pErr,&pConn))
   {
      AllocationError("connection handle");
   }

   if (TCIAllocStatement(pConn,pErr,&pStmt))
   {
      AllocationError("statement handle");
   }

   if (TCIAllocResultSet(pStmt,pErr,&pRes))
   {
      AllocationError("resultset handle");
   }
}

/* ******************************************************************** **
** @@ Cleanup()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void Cleanup()
{
   TCIFreeResultSet(pRes);
   TCIFreeStatement(pStmt);
   TCIFreeConnection(pConn);
   TCIFreeTransaction(pTa);
   TCIFreeError(pErr);
   TCIFreeEnvironment(pEnv);
}

/* ******************************************************************** **
** @@ TBEerror()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static TCIState TBEerror(TCIState rc)
{
   if (rc && (TCI_NO_DATA_FOUND != rc))
   {
      TCIState    erc = TCI_SUCCESS;

      char     errmsg[1024];
      char     sqlcode[6];

      Error    tberr = E_NO_ERROR;

      sqlcode[5] = 0;

      erc = TCIGetError(pErr,1,1,errmsg,sizeof(errmsg),&tberr,sqlcode);

      if (erc)
      {
         erc = TCIGetEnvironmentError(pEnv,1,errmsg,sizeof(errmsg),&tberr,sqlcode);

         if (erc)
         {
            // Error !
            ASSERT(0);
            printf("Can't get error info for error %d (reason: error %d)\n",rc,erc);
            exit(rc);
         }
      }

      // Error !
      ASSERT(0);
      printf("Transbase Error %d (SQLCode %s):\n%s\n",tberr,sqlcode,errmsg);

      exit(rc);
   }

   return rc;
}

/* ******************************************************************** **
** @@ TD_Dump()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void TD_Write()
{
   TOF_ART_LOOKUP    tofArtLookup;

   memset(&tofArtLookup,0,sizeof(TOF_ART_LOOKUP));

   int      iRow  = 0;

   Int2     Indicator = 0;

   TCIState    Err;

   // 1. ARL_ART_ID
   TBEerror(TCIBindColumnA(pRes,1,&tofArtLookup._iARL_ART_ID,sizeof(Int4),NULL,TCI_C_SINT4,&Indicator));
   // 2. ARL_SEARCH_NUMBER
   TBEerror(TCIBindColumnA(pRes,2,&tofArtLookup._pszARL_SEARCH_NUMBER,sizeof(Int4),NULL,TCI_C_SCHARPTR,&Indicator));
   // 3. ARL_KIND
   TBEerror(TCIBindColumnA(pRes,3,&tofArtLookup._cARL_KIND,sizeof(Int1),NULL,TCI_C_SINT1,&Indicator));
   // 4. ARL_CTM
   TBEerror(TCIBindColumnA(pRes,4,&tofArtLookup._biARL_CTM,sizeof(Bits),NULL,TCI_C_TBBITSLONG,&Indicator));
   // 5. ARL_BRA_ID
   TBEerror(TCIBindColumnA(pRes,5,&tofArtLookup._iARL_BRA_ID,sizeof(Int2),NULL,TCI_C_SINT2,&Indicator));
   // 6. ARL_DISPLAY_NR
   TBEerror(TCIBindColumnA(pRes,6,&tofArtLookup._pszARL_DISPLAY_NR,sizeof(Int4),NULL,TCI_C_SCHARPTR,&Indicator));
   // 7. ARL_DISPLAY
   TBEerror(TCIBindColumnA(pRes,7,&tofArtLookup._iARL_DISPLAY,sizeof(Int2),NULL,TCI_C_SINT2,&Indicator));
   // 8. ARL_BLOCK
   TBEerror(TCIBindColumnA(pRes,8,&tofArtLookup._iARL_BLOCK,sizeof(Int2),NULL,TCI_C_SINT2,&Indicator));
   // 9. ARL_SORT
   TBEerror(TCIBindColumnA(pRes,9,&tofArtLookup._iARL_SORT,sizeof(Int2),NULL,TCI_C_SINT2,&Indicator));

   fprintf(_pOut,"\"##\", \"ARL_ART_ID\", \"ARL_SEARCH_NUMBER\", \"ARL_KIND\", \"ARL_CTM\", \"ARL_BRA_ID\", \"ARL_DISPLAY_NR\", \"ARL_DISPLAY\", \"ARL_BLOCK\", \"ARL_SORT\"\n");

   while ((Err = TCIFetchA(pRes,1,TCI_FETCH_NEXT,0)) == TCI_SUCCESS) 
   { 
      fprintf(_pOut,"%d",++iRow);

      // 1. ARL_ART_ID
      fprintf(_pOut,", %ld",tofArtLookup._iARL_ART_ID);
      // 2. ARL_SEARCH_NUMBER
      fprintf(_pOut,", \"%s\"",tofArtLookup._pszARL_SEARCH_NUMBER);
      // 3. ARL_KIND
      fprintf(_pOut,", %d",tofArtLookup._cARL_KIND);

      // 4. ARL_CTM
      int   iBytes = BITS_TO_BYTE(tofArtLookup._biARL_CTM.length);

      BYTE*    pArr = tofArtLookup._biARL_CTM.bits;

      fprintf(_pOut,", ",tofArtLookup._biARL_CTM);

      for (int ii = 0; ii < iBytes; ++ii)
      {
         fprintf(_pOut,"%02X",pArr[ii]);
      }

      // 5. ARL_BRA_ID
      fprintf(_pOut,", %d",tofArtLookup._iARL_BRA_ID);
      // 6. ARL_DISPLAY_NR
      fprintf(_pOut,", \"%s\"",tofArtLookup._pszARL_DISPLAY_NR  ?  tofArtLookup._pszARL_DISPLAY_NR  :  "");
      // 7. ARL_DISPLAY
      fprintf(_pOut,", %d",tofArtLookup._iARL_DISPLAY);
      // 8. ARL_BLOCK 
      fprintf(_pOut,", %d",tofArtLookup._iARL_BLOCK);
      // 9. ARL_SORT
      fprintf(_pOut,", %d",tofArtLookup._iARL_SORT);

      fprintf(_pOut,"\n");
   } 

   if (Err != TCI_NO_DATA_FOUND) 
   {
     TBEerror(Err); 
   }
}

/* ******************************************************************** **
** @@ TD_Dump()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void TD_Dump(DWORD dwIdx)
{
   ASSERT(dwIdx);

   char     pszFile[MAX_PATH];

   sprintf(pszFile,"%s_%02d.csv",_pszTable,dwIdx);

   DWORD    dwFrom = TD_MAX_SEGMEBT_RECORDS_ID * (dwIdx - 1);
   DWORD    dwTo   = TD_MAX_SEGMEBT_RECORDS_ID * dwIdx + 1;

   _pOut = fopen(pszFile,"wt");

   if (!_pOut)
   {
      // Error !
      ASSERT(0);
      printf("Err: Can't open [%s] for write.\n",pszFile);
      return;
   }

   TOF_ART_LOOKUP    tofArtLookup;

   memset(&tofArtLookup,0,sizeof(TOF_ART_LOOKUP));

   Init();

   TBEerror(TCIConnect(pConn,TD_DB_NAME));
   TBEerror(TCILogin(pConn,TD_TB_LOGIN,TD_TB_PASSWORD));
   
   char     pszQuery[MAX_PATH];

   sprintf(pszQuery,"SELECT * FROM TOF_ART_LOOKUP WHERE ((ARL_ART_ID > %lu) and (ARL_ART_ID < %lu)) ORDER BY ARL_ART_ID",dwFrom,dwTo);

   TBEerror(TCIExecuteDirectA(pRes,pszQuery,1,0));

   TD_Write();

   fclose(_pOut);
   _pOut = NULL;
}

/* ******************************************************************** **
** @@ ShowHelp()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void ShowHelp()
{
   const char pszCopyright[] = "-*-   tof_art_lookup 1.0  *   Copyright (c) Gazlan, 2015   -*-";
   const char pszDescript [] = "TECDOC_CD_3_2015 DB TOF_ART_LOOKUP dumper";
   const char pszE_Mail   [] = "complains_n_suggestions direct to gazlan@yandex.ru";

   printf("%s\n\n",pszCopyright);
   printf("%s\n\n",pszDescript);
   printf("Usage: tof_art_lookup.com\n\n");
   printf("%s\n",pszE_Mail);
}

/* ******************************************************************** **
** @@ main()
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

int main(int argc,char** argv)
{           
   if (argc > 1)
   {
      ShowHelp();
      return 0;
   }

   if (argc == 2)
   {
      if ((!strcmp(argv[1],"?")) || (!strcmp(argv[1],"/?")) || (!strcmp(argv[1],"-?")) || (!stricmp(argv[1],"/h")) || (!stricmp(argv[1],"-h")))
      {
         ShowHelp();
         return 0;
      }
   }

   Init();

   TBEerror(TCIConnect(pConn,TD_DB_NAME));
   TBEerror(TCILogin(pConn,TD_TB_LOGIN,TD_TB_PASSWORD));
   
   // 1..19
   TD_Dump(1);

   TBEerror(TCICloseA(pRes));

   TBEerror(TCICloseA(pRes));
   TBEerror(TCILogout(pConn));
   TBEerror(TCIDisconnect(pConn));

   Cleanup();

   return 0;
}
