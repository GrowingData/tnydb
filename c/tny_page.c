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

/* Same as tny_page, but without references to Column */

#include "tny_page.h"

#define FAST_LOG2(x) (sizeof(unsigned long)*8 - 1 - __builtin_clzl((unsigned long)(x)))
#define FAST_LOG2_UP(x) (((x) - (1 << FAST_LOG2(x))) ? FAST_LOG2(x) + 1 : FAST_LOG2(x))

static inline int _log2(unsigned int v) {
	const unsigned int b[] = { 0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000 };
	const unsigned int S[] = { 1, 2, 4, 8, 16 };
	int i;
	unsigned int r = 0; // result of log2(v) will go here
	for (i = 4; i >= 0; i--) // unroll for speed...
			{
		if (v & b[i]) {
			v >>= S[i];
			r |= S[i];
		}
	}
	return r;
}

tny_page *tny_page_new(int key_count, int length) {
	tny_page *page = tny_malloc_data(sizeof(tny_page), "tny_page.create");

	page->length = length;
//	page->depth = _log2(key_count) + 1;
	page->depth = FAST_LOG2_UP(key_count);

	// We use an array of arrays here, so that its simple for us to add
	// new levels as more keys are created
	page->data = tny_malloc_data(sizeof(u64*) * page->depth, "tny_page->data[]");

//	if (allocate > 0) {
	for (int d = 0; d < page->depth; d++) {
//		page->data[d] = tny_calloc_data(sizeof(u64), COLPAGE_LENGTH_WORDS, "tny_page->data[i]");
		page->data[d] = tny_calloc_aligned(COLPAGE_LENGTH_WORDS,  "tny_page->data[i]");
	}
//	}

	return page;
}

// Append a single value (where the value is the "idx" in col->keys )
int tny_page_append(tny_page *page, int idx) {

	// This page is too damn big!!!
	if (page->length >= COLPAGE_LENGTH) {
		return -1;
	}

	// Check out whether we need to add a new line of
	// depth here...
	int newDepth = _log2(idx) + 1;
//	int newDepth = FAST_LOG2_UP(idx)+1;

//	int added = 0;
	if (page->depth < newDepth) {

		// Yup, lets add another row of depth....
		u64** tmpData = tny_malloc_data(sizeof(u64*) * newDepth, "tny_page_append->data[]");

		// Point update my pointers in tmp to point to my existing data...
		for (int i = 0; i < page->depth; i++) {
			tmpData[i] = page->data[i];
		}

		// Define new rows of depth...
		for (int i = page->depth; i < newDepth; i++) {
//			tmpData[i] = tny_calloc_data(sizeof(u64), COLPAGE_LENGTH_WORDS, "tny_page_append->data[i]");
			tmpData[i] = tny_calloc_aligned(COLPAGE_LENGTH_WORDS,  "tny_page_append->data[i]");
		}
		// Free the old array pointing to our pointers
		tny_free_data(page->data, sizeof(u64*) * page->depth, "tny_page_append->data");
		page->data = tmpData;
		page->depth = newDepth;
//		added = 1;
	}

	// Work out where we are up to in our little page
	int word = page->length / COLPAGE_WORD_LENGTH;
	int wordPos = page->length % COLPAGE_WORD_LENGTH;

	for (int d = 0; d < page->depth; d++) {
		if ((idx & (1ull << d)) != 0) {
			// Set the bit to 1, it should already be 0 coz of calloc
			page->data[d][word] |= (1ull << wordPos);
		}
	}
//	int real = tny_page_access(page, page->length);
//	page->length++;
//	return  (added==1) ? -real: real;
	page->length++;
	return page->length;
}

int tny_page_access(tny_page *page, int rowIndex) {
	int keyIndex = 0;

	int word = rowIndex / COLPAGE_WORD_LENGTH;
	int wordPos = rowIndex % COLPAGE_WORD_LENGTH;

	for (int d = 0; d < page->depth; d++) {
		if ((page->data[d][word] & (1ull << wordPos)) != 0) {
			keyIndex |= 1ull << d;
		}
	}

	return keyIndex;

}

// Return a bitpage of all the places where keyIndex is found within our
// bitpage
u64 * tny_page_seek(tny_page *page, int keyIndex) {
	u64 * restrict result = tny_bitmap_create_ones();
	return tny_page_seek_and(page, keyIndex, result);
}

// Return a bitpage of all the places where keyIndex is found within our
// bitpage
u64 * tny_page_seek_or(tny_page *page, int keyIndex, u64 * restrict result) {

	// Make a little cache...
	int keyBitMask[page->depth];
	for (int j = 0; j < page->depth; j++) {
		keyBitMask[j] = (keyIndex & (1 << j));
	}

	// Do we have it in our key index?
	if (keyIndex >= 0) {

		// Here we walk the length of the array completely, so that it can be vectorized
		// and much more FASTER!
		u64 * restrict cur = page->data[0];

		if (keyBitMask[0] == 0) {
			for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
				result[w] = ~cur[w];
		} else {
			for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
				result[w] = cur[w];
		}

		for (int d = 1; d < page->depth; d++) {
			u64 * restrict cur = page->data[d];
			if (keyBitMask[d] == 0) {
				for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
					result[w] |= ~cur[w];
			} else {
				for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
					result[w] |= cur[w];
			}

		}

	}
	return result;
}

u64 * tny_page_seek_and(tny_page *page, int keyIndex, u64 * restrict result) {

	// Make a little cache...
	int keyBitMask[page->depth];
	for (int j = 0; j < page->depth; j++) {
		keyBitMask[j] = (keyIndex & (1 << j));
	}

	// Here we walk the length of the array completely, so that it can be vectorized
	// and much more FASTER!

//	if (keyBitMask[0] == 0) {
//		for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
//			result[w] = ~cur[w];
//	} else {
//		for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
//			result[w] = cur[w];
//	}

	for (int d = 0; d < page->depth; d++) {
		u64 * restrict cur = page->data[d];
		if (keyBitMask[d] == 0) {
			for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
				result[w] &= ~cur[w];
		} else {
			for (int w = 0; w < COLPAGE_LENGTH_WORDS; w++)
				result[w] &= cur[w];
		}

	}

	return result;
}

u64* tny_page_distinct(tny_page *page, u64 *bitmap, int *bit_length) {

	(*bit_length) = (1ull << page->depth);
	int word_length = ((*bit_length) / COLPAGE_WORD_LENGTH) + 1;
//	u64 * dist_bmp = tny_calloc_data(sizeof(u64), word_length, "tny_page_distinct");
	u64 * dist_bmp = tny_calloc_aligned(word_length, "tny_page_distinct");


	int word_pos = 0;

//	printf("tny_page_distinct.values -> {");
	for (int i = 0; i < COLPAGE_LENGTH_WORDS && word_pos < page->length; i++) {
		u64 word = bitmap[i];

		while (word != 0) {
			int pos = __builtin_ctzll(word);
			if (pos + word_pos >= page->length) {
				//Too far
				break;
			}

			word &= ~(1ull << pos);

			int val = tny_page_access(page, pos + word_pos);

//			printf(" {%d, %d}, ", val, pos);

			int bmp_word_pos = val / 64;
			int bmp_pos = val % 64;

			dist_bmp[bmp_word_pos] |= 1ull << bmp_pos;
		}
		word_pos += COLPAGE_WORD_LENGTH;

	}
//	printf("}\n");

	return dist_bmp;

}

void tny_page_array_free(int * toFree, int length) {
	tny_free_data(toFree, sizeof(int) * length, "tny_page_array_free");
}

int * tny_page_select(tny_page *page, u64 * bitmap, int *value_count) {
	(*value_count) = tny_popcnt(bitmap);
	int * result = tny_calloc_data(sizeof(int), (*value_count), "tny_page_select.result");
//	int * result = tny_calloc_aligned(sizeof(int), (*value_count), "tny_page_select.result");

	int result_idx = 0;
	int word_pos = 0;


	for (int i = 0; i < COLPAGE_LENGTH_WORDS && word_pos < page->length; i++) {
		u64 word = bitmap[i];


		while (word != 0) {
			int pos = __builtin_ctzll(word);
			if (pos + word_pos >= page->length) {
				//Too far
				break;
			}
			word &= ~(1ull << pos);

			result[result_idx] = tny_page_access(page, pos + word_pos);
			result_idx++;
		}
		word_pos += COLPAGE_WORD_LENGTH;

	}

	return result;

}

extern int tny_page_depth(tny_page *page) {
	return page->depth;
}

extern u64* tny_page_depth_data(tny_page *page, int depth) {
	return page->data[depth];
}
extern void tny_page_set_data(tny_page *page, int depth, u64* bitmap) {

	if (page->depth <= depth) {
		int newDepth = depth + 1;

		// Yup, lets add another row of depth....
		u64** tmpData = tny_malloc_data(sizeof(u64*) * newDepth, "tny_page_append->data[]");

		// Point update my pointers in tmp to point to my existing data...
		for (int i = 0; i < page->depth; i++) {
			tmpData[i] = page->data[i];
		}

		// Define new rows of depth...
		for (int i = page->depth; i < newDepth; i++) {
//			tmpData[i] = tny_calloc_data(sizeof(u64), COLPAGE_LENGTH_WORDS, "tny_page_set_data->data[i]");
			tmpData[i] = tny_calloc_aligned(COLPAGE_LENGTH_WORDS, "tny_page_set_data->data[i]");

		}
		tny_free_data(page->data, sizeof(u64*) * page->depth, "tny_page_set_data->data");

		page->depth = newDepth;
		page->data = tmpData;
	}

	page->data[depth] = bitmap;
}

