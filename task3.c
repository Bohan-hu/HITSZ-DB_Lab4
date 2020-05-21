#include "Tuple.h"
#include "extmem.h"
#include <stdio.h>
#include <stdlib.h>

int cmpTule(const void *p1, const void *p2) {
    Tuple t1 = *(Tuple *) p1;
    Tuple t2 = *(Tuple *) p2;
    return t1.a - t2.a;
}

int sort_8_block(int block_num, int dst_block_num, Buffer *buf) {
    unsigned char *blk[8];
    for (int i = 0; i < 8; i++) {
        blk[i] = readBlockFromDisk(block_num + i, buf);
        Tuple t;
        for (int j = 0; j < 7; j++) {
            t = getTuple_str(blk[i], j);
            setTuple_int(blk[i], j, t);
        }
    }

    Tuple temp[56];
    printf("Before Sort:\n");
    for (int i = 0; i < 8; i++) {
        for (int j = 0; j < 7; j++) {
            temp[i * 7 + j] = getTuple_t(blk[i], j);
        }
    }
    qsort(temp, 56, sizeof(Tuple), cmpTule);

    for (int i = 0; i < 8; i++) {
        for (int j = 0; j < 7; j++) {
            Tuple t = temp[i * 7 + j];
            setTuple_str(blk[i], j, t);
        }
        if (writeBlockToDisk(blk[i], dst_block_num + i, buf) != 0) {
            perror("Writing Block Failed!\n");
            return -1;
        }
    }
    return 0;
}

void merge_groups(int start_block_num, int num_groups, int dst_start_block_num, Buffer *buf) {
    int *group_start_block_num = malloc(sizeof(int) * (num_groups + 1));
    for (int i = 0; i < num_groups + 1; i++) {
        group_start_block_num[i] = start_block_num + 8 * i;
//        printf("Group %d starts from blk:%d\n", i, group_start_block_num[i]);
    }
    // 初始化比较缓冲区和输出缓冲区
    Tuple *compare_buffer = (Tuple *) getNewBlockInBuffer(buf);
    Tuple *output_buffer = (Tuple *) getNewBlockInBuffer(buf);
    unsigned char **subset_buffers_ptr = malloc(sizeof(unsigned char *) * num_groups);
    int output_buffer_cnt = 0;
    int *subset_buffer_read_pos = malloc(sizeof(int) * num_groups);       // 组对应缓冲区的读指针
    int *group_read_blk_num = malloc(sizeof(int) * num_groups);           // 组读到的块号
    int output_blk_cnt = 0;
    for (int i = 0; i < num_groups; i++) {
        group_read_blk_num[i] = group_start_block_num[i];
        subset_buffer_read_pos[i] = 0;
    }

    // 初始化num_groups个暂存区
    for (int i = 0; i < num_groups; i++) {
        // Read the block to the buffer
        subset_buffers_ptr[i] = readBlockFromDisk(group_start_block_num[i], buf);
        // subset_buffers_ptr[i] = 0;
        if (!subset_buffers_ptr[i]) {
            perror("Error reading block!");
            return;
        }
        // Convert the string to int
        for (int offset = 0; offset < 7; offset++) {
            setTuple_int(subset_buffers_ptr[i], offset, getTuple_str(subset_buffers_ptr[i], offset));
        }
        compare_buffer[i] = getTuple_t(subset_buffers_ptr[i], subset_buffer_read_pos[i]++);
    }

    while (1) {
        // Find the min pos
        int min_pos = 0;
        for (int i = 0; i < num_groups; i++) {
            if (compare_buffer[i].a < compare_buffer[min_pos].a) {
                min_pos = i;
            }
        }
        printf("Select the element from group %d, (%d, %d)), Block %d, next one is at %d\n", min_pos,
               compare_buffer[min_pos].a, compare_buffer[min_pos].b, group_read_blk_num[min_pos],
               subset_buffer_read_pos[min_pos]);
        if (compare_buffer[min_pos].a == 99999) {    // Reach the end....
            break;
        }
        // Send the element at the min_pos to output buffer
        output_buffer[output_buffer_cnt++] = compare_buffer[min_pos];
        // Output buffer is full?
        if (output_buffer_cnt == 7) { // Write the buffer back to the disk
            output_buffer_cnt = 0;
            qsort(output_buffer, 7, sizeof(Tuple), cmpTule);
            for (int i = 0; i < 7; i++) {
                printf("(%d,%d)\n", output_buffer[i].a, output_buffer[i].b);
            }
            for (int offset = 0; offset < 7; offset++) {
                setTuple_str((unsigned char *) output_buffer, offset,
                             getTuple_t((unsigned char *) output_buffer, offset));
            }
            writeBlockToDisk((unsigned char *) output_buffer, dst_start_block_num + output_blk_cnt, buf);
            output_blk_cnt++;
            printf("Output buffer is full\n");

            // get a new output buffer
            output_buffer = (Tuple *) getNewBlockInBuffer(buf);
        }
        // Send a new element to compare buffer
        // Reach the end of the block?
        if (subset_buffer_read_pos[min_pos] < 7) {
            compare_buffer[min_pos] = getTuple_t(subset_buffers_ptr[min_pos], subset_buffer_read_pos[min_pos]);
            //        printf("Send from group %d, pos %d\n", min_pos, subset_buffer_read_pos[min_pos]);
            subset_buffer_read_pos[min_pos]++;
        } else {
//            printf("Read a new block!\n");
            subset_buffer_read_pos[min_pos] = 0;
            group_read_blk_num[min_pos]++;
//            printf("Reading block %d, upper limit %d\n", group_read_blk_num[min_pos], group_start_block_num[min_pos + 1]);
            // See if a new block can be loaded
            if (group_read_blk_num[min_pos] < group_start_block_num[min_pos + 1]) {
                // Load a new block
//                printf("Group %d read new block: %d\n", min_pos, group_read_blk_num[min_pos]);
                freeBlockInBuffer(subset_buffers_ptr[min_pos], buf);
                subset_buffers_ptr[min_pos] = readBlockFromDisk(group_read_blk_num[min_pos], buf);
                for (int offset = 0; offset < 7; offset++) {
                    setTuple_int(subset_buffers_ptr[min_pos], offset,
                                 getTuple_str(subset_buffers_ptr[min_pos], offset));
                }
                compare_buffer[min_pos] = getTuple_t(subset_buffers_ptr[min_pos], subset_buffer_read_pos[min_pos]);
                subset_buffer_read_pos[min_pos]++;
            } else {
                // No more blocks
                compare_buffer[min_pos].a = 99999;
            }
        }
    }
}

int main(int argc, char **argv) {
    Buffer buf; /* A buffer */
    int i = 0;
    /* Initialize the buffer */
    if (!initBuffer(520, 64, &buf)) { // Buffer size=520, blkSize=64
        perror("Buffer Initialization Failed!\n");
        return -1;
    }
    // Sort the block(8 blocks per group)
    sort_8_block(1, 100, &buf);
    sort_8_block(1 + 8, 100 + 8, &buf);
    merge_groups(100, 2, 200, &buf);
    int blk_cnt;
    unsigned char *blk;
    for (blk_cnt = 1; blk_cnt <= 16; blk_cnt++) {
        if (blk_cnt < 16) {
            blk = readBlockFromDisk(blk_cnt + 199, &buf);
        }
        printf("读入数据块 %d\n", blk_cnt);
        showBlock_str(blk);
        freeBlockInBuffer(blk, &buf);
    }
}