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

#include <stdlib.h>
#include <stdio.h>
#include "tny_map.h"

int tny_map_check_resize(tny_map *list, int newLength) {
	if (newLength >= list->allocated_length) {
		int oldLength = list->allocated_length;
		int increaseBy = newLength * 0.2;
		if (increaseBy < 3) {
			increaseBy = 3;
		}
		int oldAllocated = list->allocated_length;
		list->allocated_length = newLength + increaseBy;

		if (oldLength != list->allocated_length) {
			// We need to create a new array I reckon...
			tny_kv * new = tny_malloc_data(list->allocated_length * sizeof(tny_kv), "tny_map_check_resize.new");
			for (int i = 0; i < oldAllocated; i++) {
				new[i] = list->values[i];
			}

			if (list->values != NULL) {
				tny_free_data(list->values, oldAllocated * sizeof(tny_kv), "tny_map_check_resize");
			}
			list->values = new;

//			printf("tny_map resized to: %i\n", list->allocated_length);
		}
	}

	return 0;
}

void tny_map_push(tny_map *list, tny_kv value) {
	tny_map_check_resize(list, list->length + 1);

	if (list->length >= list->allocated_length) {
		printf("Buffer overrun in tny_map_push: list->length >= list->allocated_length (%i >= %i)\nExiting!\n",
				list->length, list->allocated_length);
		exit(-1);
	}

	list->values[list->length] = value;
	list->length++;
}

void tny_map_insert(tny_map *list, int index, tny_kv value) {

	int biggest = list->length + 1 > index + 1 ? list->length + 1 : index + 1;

	tny_map_check_resize(list, biggest);

	if (index >= list->allocated_length) {
		printf("Buffer overrun in tny_map_push: list->length >= list->allocated_length (%i >= %i)\nExiting!\n",
				list->length, list->allocated_length);
		exit(-1);
	}

	for (int i = list->length - 1; i >= index; i--) {
		list->values[i + 1] = list->values[i];
	}

	list->values[index] = value;

	list->length++;

}

void tny_map_set(tny_map *list, int index, tny_kv value) {

	int biggest = list->length + 1 > index + 1 ? list->length + 1 : index + 1;

	tny_map_check_resize(list, biggest);

	if (index >= list->allocated_length) {
		printf("Buffer overrun in tny_map_push: list->length >= list->allocated_length (%i >= %i)\nExiting!\n",
				list->length, list->allocated_length);
		exit(-1);
	}

	list->values[index] = value;

	if (index < list->length) {
		list->length++;
	} else {
		list->length = index + 1;
	}

}
tny_kv tny_map_get(tny_map *list, int index) {

	if (index > list->length) {
		fprintf(stderr, "ERROR: Couldn't realloc memory!\n");
		tny_kv val;
		val.key=-1;
		val.value=-1;

		return val;
	}

	return list->values[index];
}

int tny_map_binary_find(tny_map *list, int seeking_key) {
	int l = 0;
	int r = list->length;
	int m = 0;

	if (r == 0) {
		return ~0;
	}

	while (seeking_key != list->values[m].key && l <= r) {
		m = (l + r) / 2;
		if (m >= list->length)
			break;
		if (seeking_key < list->values[m].key)
			r = m - 1;
		if (seeking_key > list->values[m].key)
			l = m + 1;
	}

	if (l <= r && m < list->length) {
		//printf("tny_map_binary_find: Found (Seeking: %i, l: %i, m: %i,r: %i, Length: %i)\n", seeking, l, m, r, list->length);
		return m;
	} else {
		//printf("tny_map_binary_find: Not found (Seeking: %i, l: %i, m: %i,r: %i, Length: %i)\n", seeking, l, m, r, list->length);
		return ~l;
	}

}

int tny_map_find(tny_map *list, int seeking) {
	return tny_map_binary_find(list, seeking);
}
tny_map *tny_map_new_allocated(int size) {

	tny_map *list = tny_malloc_data(sizeof(tny_map), "tny_map_new_allocated.list");
	list->values = tny_calloc_data(sizeof(tny_kv), size, "tny_map_new_allocated.list->values");
	list->allocated_length = size;
	list->length = 0;

	return list;
}

tny_map *tny_map_new() {
	tny_map *list = tny_malloc_data(sizeof(tny_map), "tny_map_new");
	list->allocated_length = 0;
	list->length = 0;
	list->values = NULL;
	return list;
}

tny_map *tny_map_create(tny_kv * values, int length){
	tny_map *list = tny_map_new_allocated(length);
	for (int i =0; i < length; i++){
		list->values[i] = values[i];
	}
	list->length = length;
	return list;
}



void tny_map_free(tny_map * toFree) {
	tny_free_data(toFree->values, toFree->allocated_length * sizeof(int), "tny_map_free (values)");
	tny_free_data(toFree, sizeof(tny_map), "tny_map_free");
}

