#ifndef TUPLE_H
#define TUPLE_H

#include <stdio.h>
#include <stdlib.h>
#include "extmem.h"

typedef struct _tuple {
    int a;
    int b;
} Tuple;

void int2str(unsigned char *dst, int a) {
    int base = 1000;
    for (int i = 0; i < 4; i++) {
        dst[i] = a / base + '0';
        a %= base;
        base /= 10;
    }
}

int str2int(unsigned char *src) {
    char str[5];
    str[4] = 0;
    for (int k = 0; k < 4; k++) {
        str[k] = *(src + k);
    }
    return atoi(str);
}

Tuple getTuple_str(unsigned char *blk, int tuple_num) {
    Tuple t;
    t.a = str2int(blk + tuple_num * 8);
    t.b = str2int(blk + tuple_num * 8 + 4);
    return t;
}

Tuple getTuple_t(unsigned char *blk, int tuple_num) {
    Tuple t;
    t.a = *((int *) (blk + tuple_num * 8));
    t.b = *((int *) (blk + tuple_num * 8 + 4));
    return t;
}


void setTuple_str(unsigned char *blk, int offset, Tuple t) {
    int2str(blk + 8 * offset, t.a);
    int2str(blk + 8 * offset + 4, t.b);
}

void setTuple_int(unsigned char *blk, int offset, Tuple t) {
    *(int *) (blk + 8 * offset) = t.a;
    *(int *) (blk + 8 * offset + 4) = t.b;
}

void convert_blk_2int(Tuple *blk, int nums) {
    for (int offset = 0; offset < nums; offset++) {
        setTuple_int((unsigned char *) blk, offset, getTuple_str((unsigned char *) blk, offset));
    }
}

void showBlock_str(unsigned char *blk) {
    Tuple t;
    for (int i = 0; i < 8; i++) {
        t = getTuple_str(blk, i);
        printf("(%d,%d)\n", t.a, t.b);
    }
}

#endif