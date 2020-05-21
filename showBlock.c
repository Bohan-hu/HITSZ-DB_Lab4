#include "Tuple.h"
#include "extmem.h"
#include <stdio.h>
#include <stdlib.h>
int cmpTule(const void* p1, const void* p2)
{
    Tuple t1 = *(Tuple*)p1;
    Tuple t2 = *(Tuple*)p2;
    return t1.a - t2.a;
}

int main(int argc, char** argv)
{
    Buffer buf; /* A buffer */
    unsigned char* blk; /* A pointer to a block */
    int i = 0;
    unsigned int dst_blk_offset = 0;
    /* Initialize the buffer */
    if (!initBuffer(520, 64, &buf)) { // Buffer size=520, blkSize=64
        perror("Buffer Initialization Failed!\n");
        return -1;
    }
    unsigned char* dst_blk = getNewBlockInBuffer(&buf);
    // int blkNum = 1;
    // /* Read the block from the hard disk */
    // if ((blk = readBlockFromDisk(blkNum, &buf)) == NULL) {
    //     perror("Reading Block Failed!\n");
    //     return -1;
    // }

    /* Process the data in the block */
    int X = -1;
    int Y = -1;
    int addr = -1;

    char str[5];
    char res_str[5];
    int blk_cnt;
    int res_cnt = 0;
    Tuple t;
    for (blk_cnt = 1; blk_cnt <= 16; blk_cnt++) {
        if (blk_cnt < 16) {
            blk = readBlockFromDisk(blk_cnt+999, &buf);
        }
        printf("读入数据块 %d\n", blk_cnt);
        showBlock_str(blk);
        freeBlockInBuffer(blk, &buf);
    }

    printf("\n");
    printf("IO's is %ld\n", buf.numIO); /* Check the number of IO's */

    return 0;
}
