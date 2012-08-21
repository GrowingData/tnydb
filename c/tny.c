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

#include "tny.h"

extern int tny_popcnt(u64* bitmap) {
	int count = 0;

	// We can do everything in words as we our BITMAP is always
	// initialized with zero's
	for (int i = 0; i < COLPAGE_LENGTH_WORDS; i++) {
		count += __builtin_popcountll(bitmap[i]);
	}
	return count;
}

extern int tny_popcnt_longer(u64* bitmap, int bit_length) {
	int count = 0;

	int word_length = (bit_length / COLPAGE_WORD_LENGTH) + 1;
	// We can do everything in words as we our BITMAP is always
	// initialized with zero's
	for (int i = 0; i < word_length; i++) {
		count += __builtin_popcountll(bitmap[i]);
	}
	return count;
}

extern void tny_print_bitmap(u64 *r, int length) {
	printf(" (%i) ", length);

	int rank = 0;
	if (r == NULL) {
		printf("NULL");
	} else {
		for (int i = 0; i < length; i++) {
			if (i % 8 == 0 && i != 0) {
				printf(" ");

			}
			if (tny_bit_is_set(r, i)) {
				printf("1");
				rank++;
			} else {
				printf("0");
			}
		}

	}
	printf(" (POP: %i)", rank);
}

extern int tny_bit_is_set(u64 *r, int position) {

	int wordNumber = position / COLPAGE_WORD_LENGTH;
	int bitPosition = position % COLPAGE_WORD_LENGTH;

	if ((r[wordNumber] & (1ul << bitPosition)) != 0) {
		return 1;
	} else {
		return 0;
	}

}

extern u64 * tny_bitmap_copy(u64 * from) {
	u64* r = tny_malloc_data(sizeof(u64) * COLPAGE_LENGTH_WORDS, "tny_bitmap_copy");
	for (int i = 0; i < COLPAGE_LENGTH_WORDS; i++) {
		r[i] = from[i];
	}
	return r;
}
extern void tny_bitmap_update(u64 * from, u64 * to) {
	for (int i = 0; i < COLPAGE_LENGTH_WORDS; i++) {
		to[i] = from[i];
	}
}

extern u64 * tny_bitmap_create_ones() {
	u64* r = tny_malloc_data(sizeof(u64) * COLPAGE_LENGTH_WORDS, "tny_bitmap_create_ones");
	for (int i = 0; i < COLPAGE_LENGTH_WORDS; i++) {
		r[i] = ~0ull;
	}
	return r;
}

extern u64 * tny_bitmap_create() {
	return tny_calloc_data(sizeof(u64), COLPAGE_LENGTH_WORDS, "tny_bitmap_create");
}

extern void tny_bitmap_free(u64 * toFree) {
	tny_free_data(toFree, sizeof(u64) * COLPAGE_LENGTH_WORDS, "tny_bitmap_free");
}

extern void tny_bitmap_or(u64 *restrict a, u64 *restrict b) {
	for (int i = 0; i < COLPAGE_LENGTH_WORDS; i++) {
		a[i] |= b[i];
	}
}
extern void tny_bitmap_and(u64 *restrict a, u64 *restrict b) {
	for (int i = 0; i < COLPAGE_LENGTH_WORDS; i++) {
		a[i] &= b[i];
	}
}

extern void tny_bitmap_and_longer(u64 *restrict a, u64 *restrict b, int bit_length) {
//	int count = 0;

	int word_length = (bit_length / bit_length) + 1;
	// We can do everything in words as we our BITMAP is always
	// initialized with zero's
	for (int i = 0; i < word_length; i++) {
		a[i] &= b[i];
	}
}

extern void tny_bitmap_or_longer(u64 *restrict a, u64 *restrict b, int bit_length) {
//	int count = 0;

	int word_length = (bit_length / bit_length) + 1;
	// We can do everything in words as we our BITMAP is always
	// initialized with zero's
	for (int i = 0; i < word_length; i++) {
		a[i] &= b[i];
	}
}

int * tny_bitmap_positions(u64 * bitmap, int bit_length, int *value_count) {
	(*value_count) = tny_popcnt_longer(bitmap, bit_length);
	int * result = tny_calloc_data(sizeof(int), (*value_count), "tny_page_select.result");

	int result_idx = 0;
	int word_pos = 0;

	int word_length = (bit_length / COLPAGE_WORD_LENGTH) + 1;

	for (int i = 0; i < word_length&& word_pos < bit_length; i++) {
		u64 word = bitmap[i];
		while (word != 0) {
			int pos = __builtin_ctzll(word);
			if (pos+word_pos >= bit_length){
				//Too far
				break;
			}
			word &= ~(1ull << pos);

			result[result_idx] = pos + word_pos;
			result_idx++;
		}
		word_pos += COLPAGE_WORD_LENGTH;
	}

	return result;

}
