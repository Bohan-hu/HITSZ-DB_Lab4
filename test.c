#include <stdio.h>
#include <stdlib.h>

#include "extmem.h"

int main(int argc, char **argv)
{
  Buffer buf;         /* A buffer */
  unsigned char *blk; /* A pointer to a block */
  int i = 0;

  /* Initialize the buffer */
  // 32 Bit address space
  if (!initBuffer(520, 64, &buf))
  { // Buffer size=520, blkSize=64
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  /* Get a new block in the buffer */
  blk = getNewBlockInBuffer(&buf);

  /* Fill data into the block */
  for (i = 0; i < 8; i++)
    *(blk + i) = 'a' + i;

  /* Write the block to the hard disk */
  if (writeBlockToDisk(blk, 8888, &buf) != 0)
  {
    perror("Writing Block Failed!\n");
    return -1;
  }

  int blkNum = 1;
  /* Read the block from the hard disk */
  if ((blk = readBlockFromDisk(blkNum, &buf)) == NULL)
  {
    perror("Reading Block Failed!\n");
    return -1;
  }

  /* Process the data in the block */
  int X = -1;
  int Y = -1;
  int addr = -1;

  char str[5];
  while (1)
  {
    printf("block %d:\n", blkNum);
    for (i = 0; i < 7; i++)
    {
      for (int k = 0; k < 4; k++)
      {
        str[k] = *(blk + i * 8 + k);
      }
      X = atoi(str);
      for (int k = 0; k < 4; k++)
      {
        str[k] = *(blk + i * 8 + 4 + k);
      }
      Y = atoi(str);
      printf("(%d, %d) ", X, Y);
    }
    for (int k = 0; k < 4; k++)
    {
      str[k] = *(blk + i * 8 + k);
    }
    addr = atoi(str);
    printf("\nnext address = %d \n", addr);
    blkNum = addr;
    freeBlockInBuffer(blk, &buf);
    if ((blk = readBlockFromDisk(blkNum, &buf)) == NULL)
    {
      printf("End of the Block!\n");
      // return -1;
      break;
    }
  }

  printf("\n");
  printf("IO's is %ld\n", buf.numIO); /* Check the number of IO's */

  return 0;
}
