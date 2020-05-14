#include <stdio.h>
#include <stdlib.h>
#include "extmem.h"
#include "Tuple.h"

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
    unsigned char *dst_blk = getNewBlockInBuffer(&buf);
    // Read the block
    int blkNum =1;
    /* Read the block from the hard disk */
    if ((blk = readBlockFromDisk(blkNum, &buf)) == NULL) {
        perror("Reading Block Failed!\n");
        return -1;
    }

    /* Process the data in the block */
    int X = -1;
    int Y = -1;
    int addr = -1;

    char str[5];
    char res_str[5];
    int blk_cnt;
    int res_cnt = 0;
    Tuple t;
    for (blk_cnt = 0; blk_cnt < 48; blk_cnt++) {
        printf("读入数据块 %d\n", blkNum);
        for (i = 0; i < 7; i++) {
            t = getTuple(blk, i);
            X=t.a;
            Y=t.b;
            // printf("%d,%d",X,Y);
            if (X == atoi(argv[1]) || (X == atoi(argv[2]))){
                if(blk_cnt < 16) {
                    printf("(A=%d, B=%d)\n",X,Y);
                }
                else {
                    printf("(C=%d, D=%d)\n",X,Y);
                }
                setTuple_str(dst_blk, res_cnt, t);
                res_cnt++;
            }        
        }
        for (int k = 0; k < 4; k++) {
            str[k] = *(blk + i * 8 + k);
        }
        addr = atoi(str);
        blkNum = addr;
        freeBlockInBuffer(blk, &buf);
        if(blk_cnt != 47){
            blk = readBlockFromDisk(blkNum, &buf);
        }
    }
    showBlock(dst_blk);
    printf("\n");
    printf("IO's is %ld\n", buf.numIO); /* Check the number of IO's */

    return 0;
}
