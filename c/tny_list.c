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
#include "tny_list.h"

int tny_list_check_resize(tny_list *list, int newLength) {
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
			int * new = tny_malloc_data(list->allocated_length * sizeof(int), "tny_list_check_resize.new");
			for (int i = 0; i < oldAllocated; i++) {
				new[i] = list->values[i];
			}

			if (list->values != NULL) {
				tny_free_data(list->values, oldAllocated * sizeof(int), "tny_list_check_resize");
			}
			list->values = new;

//			printf("tny_list resized to: %i\n", list->allocated_length);
		}
	}

	return 0;
}

void tny_list_push(tny_list *list, int value) {
	tny_list_check_resize(list, list->length + 1);

	if (list->length >= list->allocated_length) {
		printf("Buffer overrun in tny_list_push: list->length >= list->allocated_length (%i >= %i)\nExiting!\n",
				list->length, list->allocated_length);
		exit(-1);
	}

	list->values[list->length] = value;
	list->length++;
}

void tny_list_insert(tny_list *list, int index, int value) {

	int biggest = list->length + 1 > index + 1 ? list->length + 1 : index + 1;

	tny_list_check_resize(list, biggest);

	if (index >= list->allocated_length) {
		printf("Buffer overrun in tny_list_push: list->length >= list->allocated_length (%i >= %i)\nExiting!\n",
				list->length, list->allocated_length);
		exit(-1);
	}

	for (int i = list->length - 1; i >= index; i--) {
		list->values[i + 1] = list->values[i];
	}

	list->values[index] = value;

	list->length++;

}

void tny_list_set(tny_list *list, int index, int value) {

	int biggest = list->length + 1 > index + 1 ? list->length + 1 : index + 1;

	tny_list_check_resize(list, biggest);

	if (index >= list->allocated_length) {
		printf("Buffer overrun in tny_list_push: list->length >= list->allocated_length (%i >= %i)\nExiting!\n",
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

int tny_list_get(tny_list *list, int index) {
	if (index > list->length) {
		fprintf(stderr, "ERROR: Couldn't realloc memory!\n");
		return (-1);
	}

	return list->values[index];
}

int tny_list_binary_find(tny_list *list, int seeking) {
	int l = 0;
	int r = list->length;
	int m = 0;

	if (r == 0) {
		return ~0;
	}

	while (seeking != list->values[m] && l <= r) {
		m = (l + r) / 2;

		if (m >= list->length)
			break;

		if (seeking < list->values[m])
			r = m - 1;
		if (seeking > list->values[m])
			l = m + 1;
	}

	if (l <= r && m < list->length) {
		//printf("tny_list_binary_find: Found (Seeking: %i, l: %i, m: %i,r: %i, Length: %i)\n", seeking, l, m, r, list->length);
		return m;
	} else {
		//printf("tny_list_binary_find: Not found (Seeking: %i, l: %i, m: %i,r: %i, Length: %i)\n", seeking, l, m, r, list->length);
		return ~l;
	}

}

int tny_list_find(tny_list *list, int seeking) {
	return tny_list_binary_find(list, seeking);
}
tny_list *tny_list_new_allocated(int size) {

	tny_list *list = tny_malloc_data(sizeof(tny_list), "tny_list_new_allocated.list");
	list->values = tny_calloc_data(sizeof(int), size, "tny_list_new_allocated.list->values");
	list->allocated_length = size;
	list->length = 0;

	return list;
}

tny_list *tny_list_new() {
	tny_list *list = tny_malloc_data(sizeof(tny_list), "tny_list_new");
	list->allocated_length = 0;
	list->length = 0;
	list->values = NULL;
	return list;
}

tny_list *tny_list_create(int * values, int length){
	tny_list *list = tny_list_new_allocated(length);
	for (int i =0; i < length; i++){
		list->values[i] = values[i];
	}
	list->length = length;
	return list;
}



#define MIN_MERGESORT_LIST_SIZE    32
void mergesort_array(int a[], int size, int temp[]) {
      int i1, i2, tempi;
      if (size < MIN_MERGESORT_LIST_SIZE) {
          /* Use insertion sort */
          int i;
          for (i=0; i < size; i++) {
             int j, v = a[i];
              for (j = i - 1; j >= 0; j--) {
                 if (a[j] <= v) break;
                  a[j + 1] = a[j];
              }
              a[j + 1] = v;
          }
          return;
      }

      mergesort_array(a, size/2, temp);
      mergesort_array(a + size/2, size - size/2, temp);
      i1 = 0;
      i2 = size/2;
      tempi = 0;
      while (i1 < size/2 && i2 < size) {
          if (a[i1] <= a[i2]) {
              temp[tempi] = a[i1];
              i1++;
         } else {
             temp[tempi] = a[i2];
              i2++;
          }
          tempi++;
      }

      while (i1 < size/2) {
          temp[tempi] = a[i1];
          i1++;
          tempi++;
      }
      while (i2 < size) {
          temp[tempi] = a[i2];
         i2++;
          tempi++;
      }

      memcpy(a, temp, size*sizeof(int));
  }

void tny_swap(int *a, int *b)
{
  int t=*a; *a=*b; *b=t;
}
void tny_quick_sort(int arr[], int beg, int end)
{
  if (end > beg + 1)
  {
    int piv = arr[beg], l = beg + 1, r = end;
    while (l < r)
    {
      if (arr[l] <= piv)
        l++;
      else
        tny_swap(&arr[l], &arr[--r]);
    }
    tny_swap(&arr[--l], &arr[beg]);
    tny_quick_sort(arr, beg, l);
    tny_quick_sort(arr, r, end);
  }
}


tny_list* tny_list_union(tny_list* a, tny_list *b){
	// Firstly, lets resize "a" for the situation where
	// all of b's items are unique
	tny_list* result = tny_list_new_allocated(a->length + b->length);

	int idx_a = 0;
	int idx_b = 0;
	int idx_r = 0;

	int b_len = b->length;
	int a_len = a->length;


	while(idx_a < a_len){

		while(idx_b < b_len && a->values[idx_a] > b->values[idx_b]) {
			result->values[idx_r] = b->values[idx_b];
			idx_b++;
			idx_r++;
		}
		if (a->values[idx_a] != b->values[idx_b]){
			result->values[idx_r] = a->values[idx_a];
			idx_r++;
		}
		idx_a++;
	}
	// At the end of A, so lets add whatever else if left in B
	while(idx_b < b_len) {
		result->values[idx_r] = b->values[idx_b];
		idx_b++;
		idx_r++;
	}

	result->length = idx_r;
	return result;
} 


tny_list *tny_list_sorted_distinct(int *list, int length){
	int *sorted = tny_malloc_data(sizeof(int) * length, "tny_list_sorted_distinct.sorted");
	int *tmpSorted = tny_malloc_data(sizeof(int) * length, "tny_list_sorted_distinct.tmpSorted");
	memcpy(sorted, list, length* sizeof(int));


	// Sort it
	// tny_quick_sort(sorted, 0, length);
	mergesort_array(sorted, length, tmpSorted);
	tny_free_data(tmpSorted, sizeof(int) * length, "tny_list_sorted_distinct.tmpSorted");

	// Extract only the unique values
	int ci=1, last=sorted[0];


	for (int i =1; i < length; i++){
		if (sorted[i] != last){
			last = sorted[i];
			sorted[ci] = sorted[i];
			ci++;
		}
	}

	// Resize the array
	sorted = tny_realloc_data(sorted, sizeof(int) * (ci), sizeof(int) * length, "tny_list_sorted_distinct.shrink");


	tny_list *result = tny_malloc_data(sizeof(tny_list), "tny_list_sorted_distinct.list");
	result->values = sorted;
	result->allocated_length = ci+1;
	result->length = ci;

	return result;
}

void tny_list_free(tny_list * toFree) {
	tny_free_data(toFree->values, toFree->allocated_length * sizeof(int), "tny_list_free (values)");
	tny_free_data(toFree, sizeof(tny_list), "tny_list_free");
}

