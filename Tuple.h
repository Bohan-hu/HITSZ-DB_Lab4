
#include <stdio.h>
#include <stdlib.h>
typedef struct _tuple{
    int a;
    int b;
} Tuple;

void int2str(unsigned char* dst, int a){
    int base = 1000;
    for(int i=0;i<4;i++){
        dst[i] = a / base + '0';
        a %= base;
        base /= 10;
    }
}

int str2int(unsigned char* src){
    char str[5];
    str[4]=0;
    for (int k = 0; k < 4; k++) {
        str[k] = *(src+k);
    }
    return atoi(str);
}

Tuple getTuple_str(unsigned char* blk, int tuple_num){
    Tuple t;
    t.a = str2int(blk+tuple_num*8);
    t.b = str2int(blk+tuple_num*8+4);
    return t;
}

Tuple getTuple_t(unsigned char* blk, int tuple_num){
    Tuple t;
    t.a = *((int*)(blk+tuple_num*8));
    t.b = *((int*)(blk+tuple_num*8+4));
    return t;
}

void setTuple_str(unsigned char* blk, int offset, Tuple t){
    int2str(blk+8*offset, t.a);
    int2str(blk+8*offset+4, t.b);
}

void setTuple_int(unsigned char* blk, int offset, Tuple t){
    *(int*)(blk+8*offset) =t.a;
    *(int*)(blk+8*offset+4) = t.b;
}

void setTuple_t(unsigned char* blk, int offset, Tuple t){
    *((Tuple*)(blk+8*offset)) = t;
}

void showBlock_str(unsigned char* blk){
    Tuple t;
    for(int i = 0;i<8;i++){
        t = getTuple_str(blk,i);
        printf("(%d,%d)\n", t.a, t.b);
    }
}

void showBlock_int(unsigned char* blk){
    for(int i = 0;i<8;i++){
        printf("(%d,%d)\n", *(int*)(blk+i*8),*((int*)(blk+i*8+4)));
    }
}