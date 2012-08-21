/*  Copyright (c) 2012 Terence Siganakis.

 This file is part of tnydb.

 TnyDB is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 TnyDB is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with TnyDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* Contact: Terence Siganakis <terence@siganakis.com> */



#ifndef TNY_LIST_H_
#define TNY_LIST_H_

#include <string.h>
 
#include "tny.h"
#include "tny_mem.h"

typedef struct {
	int length;
	int allocated_length;
	int *values;
} tny_list;


// Appends a value to the end of the list
void tny_list_push(tny_list *list, int value);

// Inserts a value into the list at the specified index,
// moving items that occur after the index to their index+i
void tny_list_insert(tny_list *list, int index, int value);

// Sets the value at index specified to the value specified
void tny_list_set(tny_list *list, int index, int value);

// Gets the value at the specified index
int tny_list_get(tny_list *list, int index);

// Located the first occurence of the specified value
// by doing a binary search. If no item is found, the not
// value (~) of where it would be found is returned.
// This functions reauires that the list is already sorted
int tny_list_find(tny_list *list, int value);


// Creates a new list containing the sorted values
// from a and b.
tny_list* tny_list_union(tny_list* a, tny_list *b);

// Creates a list and returns its reference
tny_list *tny_list_new();

// Creates a list and returns its reference
tny_list *tny_list_create(int * values, int length);

// Creates a list that is pre-allocated to the size specified
tny_list *tny_list_new_allocated(int size);


// Creates a list of the disctinct values from *list, sorted ascending
tny_list *tny_list_sorted_distinct(int *list, int length);


// Frees memory used by this list
void tny_list_free(tny_list * toFree) ;


#endif /* TNY_LIST_H_ */
