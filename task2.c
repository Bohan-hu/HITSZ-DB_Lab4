#include "Tuple.h"
#include "extmem.h"
#include <stdio.h>
#include <stdlib.h>

int cmpTule(const void *p1, const void *p2) {
    Tuple t1 = *(Tuple *) p1;
    Tuple t2 = *(Tuple *) p2;
    return t1.a - t2.a;
}

int main(int argc, char **argv) {
    Buffer buf; /* A buffer */
    unsigned char *blk; /* A pointer to a block */
    int i = 0;
    unsigned int dst_blk_offset = 0;
    /* Initialize the buffer */
    if (!initBuffer(520, 64, &buf)) { // Buffer size=520, blkSize=64
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    /* Process the data in the block */
    // Sort all the blocks in the relationship R
    int blk_cnt;
    Tuple t;
    for (blk_cnt = 1; blk_cnt <= 16; blk_cnt++) {
        blk = readBlockFromDisk(blk_cnt, &buf);
        // Convert the tuple from str to int
        for (i = 0; i < 7; i++) {
            t = getTuple_str(blk, i);
            setTuple_int(blk, i, t);
        }
        // Sort the tuple
        qsort(blk, 7, sizeof(Tuple), cmpTule);
        for (int offset = 0; offset < 7; offset++) {
            setTuple_str(blk, offset, getTuple_t(blk, offset));
        }
        // Write the sorted buffer back 
        if (writeBlockToDisk(blk, 48 + blk_cnt, &buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
        // Free the allocated block
        // freeBlockInBuffer(blk, &buf);
    }

    // For relationship R, the first time Merge

    /*
    For Relationship R, 16Blocks / 4 = 4 Blocks / Group 
    2 Blocks for compare buffer
    2 Blocks for output buffer
    */
    for (int start_block = 49; start_block < 64; start_block += 4) {
        printf("Start Block = %d", start_block);
        unsigned char *subset_buffers_ptr[4];
        Tuple *compare_buffer;
        Tuple *output_buffer;
        int output_buffer_cnt = 0;
        // Initialize subset buffers & compare buffer;
        compare_buffer = (Tuple *) getNewBlockInBuffer(&buf);
        output_buffer = (Tuple *) getNewBlockInBuffer(&buf);
        // int start_block = 48;
        int stride = 1;
        int subset_current_ptr[4] = {0};
        for (int i = 0; i < 4; i++) {
            // Read the block to the buffer
            subset_buffers_ptr[i] = readBlockFromDisk(start_block + i, &buf);
            // subset_buffers_ptr[i] = 0;
            if (!subset_buffers_ptr[i]) {
                perror("Error reading block!");
                return -1;
            }
            // Convert the string to int
            for (int offset = 0; offset < 7; offset++) {
                setTuple_int(subset_buffers_ptr[i], offset, getTuple_str(subset_buffers_ptr[i], offset));
            }
            compare_buffer[i] = getTuple_t(subset_buffers_ptr[i], subset_current_ptr[i]++);
        }

        // Load the blocks into the buffer
        int output_blk_cnt = 0;
        while (output_blk_cnt < 4) {
            // Select the min
            int min_pos = 0;
            for (int i = 0; i < 4; i++) {
                if (compare_buffer[i].a < compare_buffer[min_pos].a) {
                    min_pos = i;
                }
            }
            printf("min_pos=%d\n", min_pos);
            printf("From block %d, value=(%d,%d)\n", min_pos + 1 + start_block, compare_buffer[min_pos].a,
                   compare_buffer[min_pos].b);
            // Send the min to the output buffer
            if (output_buffer_cnt < 6) {
                output_buffer[output_buffer_cnt++] = compare_buffer[min_pos];
            } else {
                printf("the buffer is full\n");
                output_buffer[output_buffer_cnt] = compare_buffer[min_pos];
                output_buffer_cnt = 0;
                qsort(output_buffer, 7, sizeof(Tuple), cmpTule);
                for (int offset = 0; offset < 7; offset++) {
                    setTuple_str((unsigned char *) output_buffer, offset,
                                 getTuple_t((unsigned char *) output_buffer, offset));
                }
                writeBlockToDisk((unsigned char *) output_buffer, 1000 + start_block + output_blk_cnt, &buf);
                output_blk_cnt++;
                output_buffer = (Tuple *) getNewBlockInBuffer(&buf);
            }
            // Send a new element to the buffer
            if (subset_current_ptr[min_pos] < 7) {
                compare_buffer[min_pos] = getTuple_t(subset_buffers_ptr[min_pos], subset_current_ptr[min_pos]++);
            } else { // No more elements, give a special tag
                compare_buffer[min_pos].a = 99999;
            }
        }
        freeBlockInBuffer((unsigned char *) output_buffer, &buf);
        freeBlockInBuffer((unsigned char *) compare_buffer, &buf);
        for (int i = 0; i < 4; i++) {
            freeBlockInBuffer(subset_buffers_ptr[i], &buf);
        }
    }

    unsigned char *subset_buffers_ptr[4];
    Tuple *compare_buffer;
    Tuple *output_buffer;
    int output_buffer_cnt = 0;
    // Initialize subset buffers & compare buffer;
    compare_buffer = (Tuple *) getNewBlockInBuffer(&buf);
    output_buffer = (Tuple *) getNewBlockInBuffer(&buf);
    int subset_current_ptr[4] = {0};
    int disk_blk_num[4] = {1, 5, 9, 13};
    for (int i = 0; i < 4; i++) {
        // Read the block to the buffer
        subset_buffers_ptr[i] = readBlockFromDisk(1048 + disk_blk_num[i], &buf);
        if (!subset_buffers_ptr) {
            perror("Error reading block!");
            return -1;
        }
        // Convert the string to int
        for (int offset = 0; offset < 7; offset++) {
            setTuple_int(subset_buffers_ptr[i], offset, getTuple_str(subset_buffers_ptr[i], offset));
        }
        compare_buffer[i] = getTuple_t(subset_buffers_ptr[i], subset_current_ptr[i]++);
    }

    for (int i = 0; i < 4; i++) {
        printf("(%d, %d)\n=====\n", compare_buffer[i].a, compare_buffer[i].b);
    }
    // Load the blocks into the buffer
    int output_blk_cnt = 0;
    while (output_blk_cnt < 16) {
        // Select the min
        int min_pos = 0;
        for (int i = 0; i < 4; i++) {
            if (compare_buffer[i].a < compare_buffer[min_pos].a) {
                min_pos = i;
            }
        }
        printf("min_pos=%d\n", min_pos);
        printf("From block %d, value=(%d,%d)\n", disk_blk_num[min_pos], compare_buffer[min_pos].a,
               compare_buffer[min_pos].b);
        // Send the min to the output buffer
        if (output_buffer_cnt < 6) {
            output_buffer[output_buffer_cnt++] = compare_buffer[min_pos];
        } else { // the buffer is full, write to disk
            output_buffer[output_buffer_cnt] = compare_buffer[min_pos];
            printf("the buffer is full\n");
            // Reset the pointer
            output_buffer_cnt = 0;
            // Sort the buffer
            qsort(output_buffer, 7, sizeof(Tuple), cmpTule);
            // Format the string
            for (int offset = 0; offset < 7; offset++) {
                setTuple_str((unsigned char *) output_buffer, offset, getTuple_t(output_buffer, offset));
            }
            writeBlockToDisk(output_buffer, 2000 + output_blk_cnt, &buf);
            output_blk_cnt++;
            output_buffer = getNewBlockInBuffer(&buf);
        }
        // Send a new element to the buffer
        if (subset_current_ptr[min_pos] < 7) {
            compare_buffer[min_pos] = getTuple_t(subset_buffers_ptr[min_pos], subset_current_ptr[min_pos]++);
        } else if (subset_current_ptr[min_pos] == 7 && disk_blk_num[min_pos] % 4 != 0) { // Load a new block
            subset_current_ptr[min_pos] = 1;
            disk_blk_num[min_pos]++;
            freeBlockInBuffer(subset_buffers_ptr[min_pos], &buf);
            subset_buffers_ptr[min_pos] = readBlockFromDisk(disk_blk_num[min_pos] + 1048, &buf);
            printf("Read a new block!\n");
            for (int offset = 0; offset < 7; offset++) {
                setTuple_int(subset_buffers_ptr[min_pos], offset, getTuple_str(subset_buffers_ptr[min_pos], offset));
            }
            compare_buffer[min_pos] = getTuple_t(subset_buffers_ptr[min_pos], 0);
        } else { // No more elements, give a special tag
            compare_buffer[min_pos].a = 99999;
        }
    }

    printf("\n");
    printf("IO's is %ld\n", buf.numIO); /* Check the number of IO's */

    return 0;
}
