/* 	Copyright (c) 2012 Terence Siganakis.

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

 

#include "tny_mem.h"

tny_mem_statistics tny_mem_stats;

#define CACHELINE_SZ    64

void tny_mem_init() {

	tny_mem_stats.allocations = 0;
	tny_mem_stats.frees = 0;
	tny_mem_stats.bytes_total = 0;
	tny_mem_stats.bytes_current = 0;
}

void *tny_malloc(int amount, char* tag) {
	return malloc(amount);
}

void tny_free(void *ptr, char* tag) {
	free(ptr);

}

tny_mem_statistics tny_mem_get_statistics() {
	return tny_mem_stats;
}

void tny_free_data(void *ptr, int bytes, char* tag) {

	tny_mem_stats.frees++;
	tny_mem_stats.bytes_current -= bytes;

	free(ptr);
}

void *tny_malloc_aligned(int size, char* tag) {
	void *p;

	tny_mem_stats.allocations++;
	tny_mem_stats.bytes_current += size;
	tny_mem_stats.bytes_total += size;

	/* NOTE: *lev2 is 64B-aligned so as to avoid cache-misses. */
	int ret = posix_memalign((void **) &p, CACHELINE_SZ, size);

	return (ret == 0) ? p : NULL;
}

void *tny_realloc_data(void *ptr, int new_bytes, int old_bytes, char* tag) {
	int difference = new_bytes - old_bytes;
	tny_mem_stats.frees++;
	tny_mem_stats.allocations++;
	tny_mem_stats.bytes_current += difference;

	void* newptr = realloc(ptr, new_bytes);
	if (newptr != NULL) {
		return newptr;
	} else {
		fprintf(stderr, "REALLOC Failed! {new_bytes: %i, old_bytes: %i}", new_bytes, old_bytes);
		exit(-1);
	}
}
void *tny_calloc_data(int item_size, int item_count, char* tag) {
	tny_mem_stats.allocations++;
	tny_mem_stats.bytes_current += (item_size * item_count);
	tny_mem_stats.bytes_total += (item_size * item_count);

	void *ptr = calloc(item_count, item_size);

	return ptr;
}


u64 *tny_calloc_aligned(int items, char* tag) {
	u64 *p;

	tny_mem_stats.allocations++;
	tny_mem_stats.bytes_current += items *sizeof(u64);
	tny_mem_stats.bytes_total += items *sizeof(u64);

	/* NOTE: *lev2 is 64B-aligned so as to avoid cache-misses. */
	int ret = posix_memalign( (void**)&p, CACHELINE_SZ, items *sizeof(u64));
	if (ret!=0){
		return NULL;
	}

	// Set to 0
	for (int i=0; i < items; i++){
		p[i]=0;
	}

	return p;
}

void *tny_malloc_data(int bytes, char* tag) {
	tny_mem_stats.allocations++;
	tny_mem_stats.bytes_current += bytes;
	tny_mem_stats.bytes_total += bytes;

	void * ptr = malloc(bytes);

	return ptr;
}
