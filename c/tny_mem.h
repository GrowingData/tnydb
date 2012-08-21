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
 

#ifndef TNY_MEM_H_
#define TNY_MEM_H_


#include <stdlib.h>
#include <stdio.h>
#include <smmintrin.h>


typedef unsigned long long u64;

typedef struct  {
	int allocations;
	int frees;

	int bytes_total;
	int bytes_current;

} tny_mem_statistics;




void *tny_malloc(int amount, char *tag);
void tny_free(void *ptr, char *tag);


void tny_free_data(void *ptr, int bytes, char* tag);
void *tny_malloc_data(int bytes, char* tag);
void *tny_realloc_data(void *ptr, int new_bytes, int old_bytes, char* tag);
void *tny_calloc_data(int item_size, int item_count, char* tag);
void *tny_malloc_aligned(int size, char* tag);

u64 *tny_calloc_aligned(int items, char* tag);


void tny_mem_init();
tny_mem_statistics tny_mem_get_statistics();

#endif /* TNY_MEM_H_ */
