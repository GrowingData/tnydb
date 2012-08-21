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



#ifndef TNY_MAP_H_
#define TNY_MAP_H_

#include <string.h>
 
#include "tny.h"
#include "tny_mem.h"


typedef struct {
	int key;
	int value;
} tny_kv;

typedef struct {
	int length;
	int allocated_length;
	tny_kv *values;
} tny_map;


// Appends a value to the end of the list
void tny_map_push(tny_map *list, tny_kv value);

// Inserts a value into the list at the specified index,
// moving items that occur after the index to their index+i
void tny_map_insert(tny_map *list, int index, tny_kv value);

// Sets the value at index specified to the value specified
void tny_map_set(tny_map *list, int index, tny_kv value);

// Gets the value at the specified index
tny_kv tny_map_get(tny_map *list, int index);

// Located the first occurence of the specified value
// by doing a binary search. If no item is found, the not
// value (~) of where it would be found is returned.
// This functions reauires that the list is already sorted
int tny_map_binary_find(tny_map *list, int seeking_key);


// Creates a list and returns its reference
tny_map *tny_map_new();

// Creates a list and returns its reference
tny_map *tny_map_create(tny_kv * values, int length);

// Creates a list that is pre-allocated to the size specified
tny_map *tny_map_new_allocated(int size);



// Frees memory used by this list
void tny_map_free(tny_map * toFree) ;


#endif /* TNY_MAP_H_ */
