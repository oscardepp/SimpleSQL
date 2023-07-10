/*execute.c*/

//
// Project: Execution of queries for SimpleSQL
//
// Oscar Depp (Taken partially from solution code in project 2)
// Northwestern University
// CS 211, Winter 2023
//

#include <assert.h>  // assert
#include <stdbool.h> // true, false
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "ast.h"
#include "database.h"
#include "resultset.h"
#include "tokenqueue.h"
#include "util.h"

static struct ResultSet *create_dataset(struct TableMeta *tablemeta);
static void read_dataset(struct ResultSet *rs, struct TableMeta *tablemeta,
                         char *dataBuffer, int dataBufferSize);
static void check_where(struct SELECT *select, struct ResultSet *rs);
static void ints_filter_where(struct ResultSet *rs, struct EXPR *where_expr,
                              int where_col_index, int i);
static void filter_reals_where(struct ResultSet *rs, struct EXPR *where_expr,
                               int where_col_index, int i);
static void strings_filter_where(struct ResultSet *rs, struct EXPR *where_expr,
                                 int where_col_index, int i);
static void filter_col(struct TableMeta *tablemeta, struct SELECT *select,
                       struct ResultSet *rs);
static void reorder_col(struct SELECT *select, struct ResultSet *rs);
static void app_func(struct SELECT *select, struct ResultSet *rs);
static void check_lim(struct LIMIT *limit, struct ResultSet *rs);

//
// execute_query
//
// execute a select query, which for now means open the underlying
// .data file and output the first 5 lines.
//
void execute_query(struct Database *db, struct QUERY *query) {
  if (db == NULL)
    panic("db is NULL (execute)");
  if (query == NULL)
    panic("query is NULL (execute)");

  if (query->queryType != SELECT_QUERY) {
    printf("**INTERNAL ERROR: execute() only supports SELECT queries.\n");
    return;
  }
  struct SELECT *select = query->q.select; // alias for less typing:
                                           //
  // the query has been analyzed and so we know it's correct: the
  // database exists, the table(s) exist, the column(s) exist, etc.
  //
  struct TableMeta *tablemeta = NULL;
  for (int t = 0; t < db->numTables; t++) {
    if (icmpStrings(db->tables[t].name, select->table) == 0) // found it:
    {
      tablemeta = &db->tables[t];
      break;
    }
  }
  assert(tablemeta != NULL);
  //
  // (1) we need a pointer to the table meta data, so find it:
  //

  //
  // (2) open the table's data file
  //
  // the table exists within a sub-directory under the executable
  // where the directory has the same name as the database, and with
  // a "TABLE-NAME.data" filename within that sub-directory:
  //
  char path[(2 * DATABASE_MAX_ID_LENGTH) + 10];

  strcpy(path, db->name); // name/name.data
  strcat(path, "/");
  strcat(path, tablemeta->name);
  strcat(path, ".data");

  FILE *datafile = fopen(path, "r");
  if (datafile == NULL) // unable to open:
  {
    printf("**INTERNAL ERROR: table's data file '%s' not found.\n", path);
    panic("execution halted");
    exit(-1);
  }

  // part 3. create a result Set
  //
  //
  struct ResultSet *rs = create_dataset(tablemeta);

  //
  // (3) allocate a buffer for input, and start reading ALL the data:
  //
  int dataBufferSize =
      tablemeta->recordSize + 3; // ends with $\n + null terminator
  char *dataBuffer = (char *)malloc(sizeof(char) * dataBufferSize);
  if (dataBuffer == NULL)
    panic("out of memory");

  // part 4: read dataset
  while (true) {
    fgets(dataBuffer, dataBufferSize, datafile);
    if (feof(datafile)) // end of the data file, we're done
      break;
    read_dataset(rs, tablemeta, dataBuffer, dataBufferSize);
  }
  free(dataBuffer);

  // part 6: where clause
  check_where(select, rs);
  // part 7: filter columns
  // until here we're fine–> we can add a duplicate column in filter
  filter_col(tablemeta, select, rs);

  // part 8: reorganize the columns to match ast
  reorder_col(select, rs);
  // part 9: apply functions
  app_func(select, rs);
  // part 10: check limits
  check_lim(select->limit, rs);

  resultset_print(rs);
  resultset_destroy(rs);
  fclose(datafile);

  //
  // done!
  //
}

//
// create_dataset
//
// generates a dataset of columnsbased off the dimensions of the table of // the
// query
//
//
static struct ResultSet *create_dataset(struct TableMeta *tablemeta) {
  struct ResultSet *rs = resultset_create();
  for (int i = 0; i < tablemeta->numColumns; i++) {
    struct ColumnMeta *column = &tablemeta->columns[i];
    int pos = i + 1;
    int colfunc = NO_FUNCTION;
    int col_typ = column->colType;
    int colPos = resultset_insertColumn(rs, pos, tablemeta->name, column->name,
                                        colfunc, col_typ);
  }
  return rs;
}
//
// read_dataset
//
// reads each line of the data and puts the data into the result
// by each column.
//
static void read_dataset(struct ResultSet *rs, struct TableMeta *tablemeta,
                         char *dataBuffer, int dataBufferSize) {
  int rowPos = resultset_addRow(rs);
  char *cp = dataBuffer;
  char *end = cp;
  for (int i = 0; i < tablemeta->numColumns; i++) {
    struct ColumnMeta *column = &tablemeta->columns[i];
    int col_type = column->colType;
    if (col_type == 1) {
      // INT
      while (*end != ' ') {
        end++;
      }
      *end = '\0';
      int val = atoi(cp);
      resultset_putInt(rs, rowPos, i + 1, val);
      end++;
      cp = end;
    } else if (col_type == 2) {
      // REAL
      while (*end != '\0') {
        end++;
      }
      *end = '\0';
      double val = atof(cp);
      resultset_putReal(rs, rowPos, i + 1, val);
      end++;
      cp = end;
    } else if (col_type == 3) {
      // STRING
      if (*end == '"') {
        end++;
        cp = end;
        while (*end != '"') {
          end++;
        }
      } else if (*end == '\'') {
        end++;
        cp = end;
        while (*end != '\'') {
          end++;
        }
      }
      *end = '\0';
      resultset_putString(rs, rowPos, i + 1, cp);
      end = end + 2;
      cp = end;
    }
  }
}

//
// check_where
//
// Finds the column where the "where" clause is applied, and then
// filters out what the "where" operators wants to filter.
//
static void check_where(struct SELECT *select, struct ResultSet *rs) {
  if (select->where != NULL) {
    //  WHY IS LITTYPE(AST) ENUM'ed DIFFERENTLY FROM HOW COLTYPE IS ENUM'ed
    // Different where clauses: {"<", "<=", ">", ">=", "==", "!="};
    struct EXPR *where_expr = select->where->expr;
    int where_col_index =
        resultset_findColumn(rs, 1, select->table, where_expr->column->name);
    int curColType = -1;
    int i = 1;
    // check for the column type you're in and compare with the where type
    // i.e. Revenue in the where clause can be an int (10000), but the column
    // type in the resultset will only accept reals
    struct RSColumn *cur = rs->columns;
    while (cur != NULL) {
      if (where_col_index == i) {
        curColType = cur->coltype;
        break;
      }
      i++;
      cur = cur->next;
    }
    int lit_type = where_expr->litType;
    for (int i = rs->numRows; i >= 1; i--) {
      if (lit_type == 0 && curColType == 1) {

        ints_filter_where(rs, where_expr, where_col_index, i);

      } else if (lit_type == 1 || (lit_type == 0 && curColType == 2)) {
        // if its a REAL in the where, or if its a INT in the where and a
        // REAL in the column
        filter_reals_where(rs, where_expr, where_col_index, i);
      } else if (where_expr->litType == STRING_LITERAL) {
        strings_filter_where(rs, where_expr, where_col_index, i);
      }
    }
  }
}

//
// ints_filter_where
//
// filters out any ints that don't meet the where value and operator clause
//
static void ints_filter_where(struct ResultSet *rs, struct EXPR *where_expr,
                              int where_col_index, int i) {
  int row_val = resultset_getInt(rs, i, where_col_index);
  int where_val = atof(where_expr->value);
  switch (where_expr->operator) {
  case 0:
    if (row_val >= where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 1:
    if (row_val > where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 2:
    if (row_val <= where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 3:
    if (row_val < where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 4:
    if (row_val != where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 5:
    if (row_val == where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  }
}

//
// filter_reals_where
//
// filters out any reals that don't meet the where value and operator clause
//
static void filter_reals_where(struct ResultSet *rs, struct EXPR *where_expr,
                               int where_col_index, int i) {
  double row_val = resultset_getReal(rs, i, where_col_index);
  double where_val = atof(where_expr->value);
  switch (where_expr->operator) {
  case 0:
    if (row_val >= where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 1:
    if (row_val > where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 2:
    if (row_val <= where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 3:
    if (row_val < where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 4:
    if (row_val != where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 5:
    if (row_val == where_val) {
      resultset_deleteRow(rs, i);
    }
    break;
  }
}

//
// strings_filter_where
//
// filters out any strings that don't meet the where value and operator clause
//
static void strings_filter_where(struct ResultSet *rs, struct EXPR *where_expr,
                                 int where_col_index, int i) {
  char *row_val = resultset_getString(rs, i, where_col_index);
  char *where_val = where_expr->value;
  int comp = strcmp(row_val, where_val);
  switch (where_expr->operator) {
  case 0:
    if (comp > 0 || comp == 0) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 1:
    if (comp > 0) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 2:
    if (comp < 0 || comp == 0) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 3:
    if (comp < 0) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 4:
    if (comp != 0) {
      resultset_deleteRow(rs, i);
    }
    break;
  case 5:
    if (comp == 0) {
      resultset_deleteRow(rs, i);
    }
    break;
  }
  free(row_val);
}

//
// filter_col
//
// filters out and removes any column in the result set that is not // // part
// of the query, returns the size of ast columns.
//

static void filter_col(struct TableMeta *tablemeta, struct SELECT *select,
                       struct ResultSet *rs) {
  int size_table = tablemeta->numColumns;
  // int size_query = 0;
  for (int i = 0; i < size_table; i++) {
    // size_query = 0;
    bool in_ast = false;
    struct ColumnMeta colmeta = tablemeta->columns[i];
    struct COLUMN *cur = select->columns;
    while (cur != NULL) {
      if (strcasecmp(colmeta.name, cur->name) == 0) {
        in_ast = true;
      }
      // size_query++;
      cur = cur->next;
    }
    // if true it is in the list, don't do anything
    //  if false, then delete the column
    if (!in_ast) {
      int col_Pos = resultset_findColumn(rs, 1, tablemeta->name, colmeta.name);
      resultset_deleteColumn(rs, col_Pos);
    }
  }
}

// //
// // add_dups
// //
// // checks for duplicates, then adds a column at the end of the resultlist set
// //
// static void check_dups(struct SELECT *select, struct ResultSet *rs, int
// size_query) {
//   int col_index = 0;
//   int size_rs = 0;
//   struct RSColumn* rs_cur = rs->columns;
//   while(rs_cur != NULL) {
//     size_rs++;
//     rs_cur = rs_cur->next;
//   }
//   int dup[size_rs];
//   //printf("size of query: %d\n", size_query);
//   rs_cur = rs->columns;
//   while (rs_cur != NULL) {
//     int i = 0;
//     struct COLUMN *cur = select->columns;
//     while (cur != NULL) {

//       if (strcasecmp(rs_cur->colName, cur->name) == 0) {
//         i++;
//       }
//       cur = cur->next;
//     }
//     dup[col_index] = i;
//     // printf("numCols = %d\n", rs->numCols);
//     if (i != 1) {
//       //printf("%s\n", rs_cur->colName);
//       //printf("%d\n", rs_cur->coltype);
//       //add_dups(select, rs, rs_cur->colName, rs_cur->coltype);
//     }
//     // if i is not 1: do smth bitch
//     col_index++;
//     rs_cur = rs_cur->next;
//   }
//   for(int ii = 0; ii<sizeof(dup); ii++) {
//     printf("%d\n", dup[ii]);// have to add one for the column index
//   }

// }
// //
// // add_dups
// //
// // Once a duplicated column is identified, this function will duplicate its
// // contents and add it to the end of the list.
// //
// static void add_dups(struct SELECT *select, struct ResultSet *rs,
//                      char *col_name, int col_type) {
//   // use this to grab stuff and put it in
//   int og_col_index =
//       resultset_findColumn(rs, 1, rs->columns->tableName, col_name);

//   int pos = rs->numCols + 1;
//   int func = NO_FUNCTION;
//   int dup_index = resultset_insertColumn(rs, pos, rs->columns->tableName,
//   col_name, func, col_type);
//   // now we've created it, lets grab its rows and put it into the last column
//   for (int r = 1; r < rs->numRows; r++) {
//     if (col_type == 1) {
//       int int_getter = resultset_getInt(rs, r, og_col_index);
//       resultset_putInt(rs, r, dup_index, int_getter);
//     } else if (col_type == 2) {
//       double dubs_getter = resultset_getReal(rs, r, og_col_index);
//       resultset_putReal(rs, r, dup_index, dubs_getter);
//     } else {
//       printf("STRING OR ERROR*\n");
//     }
//   }
// }

//
// reorder_col
//
// reorders the columns in the order that is presented in the query.
//
static void reorder_col(struct SELECT *select, struct ResultSet *rs) {
  int i = 1;
  struct COLUMN *cur = select->columns;
  while (cur != NULL) {
    int col_index =
        resultset_findColumn(rs, 1, rs->columns->tableName, cur->name);
    resultset_moveColumn(rs, col_index, i);
    i++;
    cur = cur->next;
  }
}

//
// app_func
//
// checks whether there is a function call in the query and applies the
// function to the resultset
//
static void app_func(struct SELECT *select, struct ResultSet *rs) {
  int i = 1;
  struct COLUMN *cur = select->columns;
  while (cur != NULL) {
    if (cur->function != NO_FUNCTION) {
      resultset_applyFunction(rs, cur->function, i);
    }
    i++;
    cur = cur->next;
  }
}

//
// check_lim
//
// checks if there is a limit clause involved in the query, if there is
// limit the rows in the result set to that number.
//
static void check_lim(struct LIMIT *limit, struct ResultSet *rs) {
  if (limit != NULL) {
    int rowNums = rs->numRows;
    for (int i = rowNums; i >= limit->N + 1; i--) {
      resultset_deleteRow(rs, i);
    }
  }
}