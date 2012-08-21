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

#ifndef TNY_H_
#define TNY_H_

#include <stdlib.h>
#include <stdio.h>
#include "tny_mem.h"


#define COLPAGE_LENGTH 8192  //1024
#define COLPAGE_WORD_LENGTH 64
#define COLPAGE_LENGTH_WORDS (COLPAGE_LENGTH/COLPAGE_WORD_LENGTH)

extern void tny_print_bitmap(u64 *r, int length);
extern int tny_popcnt(u64* bitmap);
extern int tny_bit_is_set(u64* bitmap, int length);

// Bitmap functions

extern u64 * tny_bitmap_create();
extern u64 * tny_bitmap_create_ones();
extern u64 * tny_bitmap_copy(u64 * from);

extern void tny_bitmap_update(u64 * from, u64 * to);
extern void tny_bitmap_free(u64 * toFree);
extern void tny_bitmap_or(u64 * a, u64 *b);
extern void tny_bitmap_and(u64 * a, u64 *b);

extern int tny_popcnt_longer(u64* bitmap, int bit_length);
int * tny_bitmap_positions(u64 * bitmap, int bit_length, int *value_count);
extern void tny_bitmap_and_longer(u64 *restrict a, u64 *restrict b, int bit_length);
extern void tny_bitmap_or_longer(u64 *restrict a, u64 *restrict b, int bit_length);


extern void tny_print_bitmap(u64 *r, int length) ;

#endif /* TNY_H_ */
