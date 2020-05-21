#include "Tuple.h"
#include "extmem.h"
#include <stdio.h>
#include <stdlib.h>

int cmpTule(const void *p1, const void *p2) {
    Tuple t1 = *(Tuple *) p1;
    Tuple t2 = *(Tuple *) p2;
    return t1.a - t2.a;
}

void showBlocks(int start, int nums, Buffer *buf) {
    int blk_cnt;
    unsigned char *blk;
    for (blk_cnt = start; blk_cnt < start + nums; blk_cnt++) {
        blk = readBlockFromDisk(blk_cnt, buf);
        printf("读入数据块 %d\n", blk_cnt);
        showBlock_str(blk);
        freeBlockInBuffer(blk, buf);
        printf("-------------\n");
    }
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
        if (writeBlockToDisk(&blk[i], dst_block_num + i, buf) != 0) {
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
        if (compare_buffer[min_pos].a == 99999) {    // Reach the end....
            break;
        }
        // Send the element at the min_pos to output buffer
        output_buffer[output_buffer_cnt++] = compare_buffer[min_pos];
        // Output buffer is full?
        if (output_buffer_cnt == 7) { // Write the buffer back to the disk
            output_buffer_cnt = 0;
            qsort(output_buffer, 7, sizeof(Tuple), cmpTule);
            Tuple t;
            t.a = dst_start_block_num + output_blk_cnt + 1;
            t.b = 0;
            output_buffer[7] = t;
            for (int offset = 0; offset < 8; offset++) {
                setTuple_str((unsigned char *) output_buffer, offset,
                             getTuple_t((unsigned char *) output_buffer, offset));
            }
            writeBlockToDisk((unsigned char **) &output_buffer, dst_start_block_num + output_blk_cnt, buf);
            output_blk_cnt++;
            // get a new output buffer
            output_buffer = (Tuple *) getNewBlockInBuffer(buf);
        }
        // Send a new element to compare buffer
        // Reach the end of the block?
        if (subset_buffer_read_pos[min_pos] < 7) {
            compare_buffer[min_pos] = getTuple_t(subset_buffers_ptr[min_pos], subset_buffer_read_pos[min_pos]);
            subset_buffer_read_pos[min_pos]++;
        } else {
            subset_buffer_read_pos[min_pos] = 0;
            group_read_blk_num[min_pos]++;
            // See if a new block can be loaded
            if (group_read_blk_num[min_pos] < group_start_block_num[min_pos + 1]) {
                // Load a new block
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
    // Do the cleanup
    for (int i = 0; i < num_groups; i++) {
        freeBlockInBuffer((unsigned char *) subset_buffers_ptr[i], buf);
    }
    freeBlockInBuffer((unsigned char *) output_buffer, buf);
    freeBlockInBuffer((unsigned char *) compare_buffer, buf);
}

int makeIndex(int start_block, int num_blocks, int dst_block, Buffer *buf) {
    int blk_cnt;
    unsigned char *blk;
    Tuple *output_blk = (Tuple *) getNewBlockInBuffer(buf);
    int output_blk_pos = 0;
    int output_blk_cnt = 0;
    Tuple t;
    for (blk_cnt = start_block; blk_cnt < start_block + num_blocks; blk_cnt++) {
        if (output_blk == NULL) {
            output_blk = (Tuple *) getNewBlockInBuffer(buf);
        }
        blk = readBlockFromDisk(blk_cnt, buf);
        t.a = getTuple_str(blk, 0).a;
        t.b = blk_cnt;
        output_blk[output_blk_pos++] = t;
        if (output_blk_pos == 8) {
            // Write back to disk
            output_blk_pos = 0;
            writeBlockToDisk((unsigned char **) &output_blk, dst_block + output_blk_cnt, buf);
            output_blk_cnt++;
        }
        freeBlockInBuffer(blk, buf);
    }
    if (output_blk != NULL) {
        writeBlockToDisk((unsigned char **) &output_blk, dst_block + output_blk_cnt, buf);
    }
    return output_blk_cnt;
}

int locateBlkbyIndex(int search_key, int index_start_num, int num_index_blocks, Buffer *buf) {
    // 遍历index，找到满足key的最小值
    unsigned char *blk;
    int search_continue = 1;
    int blk_num;
    for (int blk_cnt = index_start_num; blk_cnt < index_start_num + num_index_blocks && search_continue; blk_cnt++) {
        blk = readBlockFromDisk(blk_cnt, buf);
        for (int i = 0; i < 8; i++) {
            if (((Tuple *) blk)[i].a >= search_key) {
                blk_num = ((Tuple *) blk)[i].b;
                search_continue = 0;
                break;
            }
        }
    }
    return blk_num - 1;
}

void searchFromBlock(int value, int start_block, Buffer *buf) {
    unsigned char *blk;
    int current_blk = start_block;
    int search_flag = 1;
    while (search_flag) {
        blk = readBlockFromDisk(current_blk, buf);
        for (int offset = 0; offset < 7; offset++) {
            setTuple_int(blk, offset,
                         getTuple_str(blk, offset));
            if (((Tuple *) blk)[offset].a == value) {
                printf("(%d, %d)\n", ((Tuple *) blk)[offset].a, ((Tuple *) blk)[offset].b);
            }
            if (((Tuple *) blk)[offset].a > value) {
                search_flag = 0;
                freeBlockInBuffer(blk, buf);
                break;
            }
        }
        current_blk++;
        freeBlockInBuffer(blk, buf);
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
    int dest_blk = 100;
    sort_8_block(1, dest_blk, &buf);
    sort_8_block(1 + 8, dest_blk + 8, &buf);
    merge_groups(dest_blk, 2, 200, &buf);
    dest_blk = 116;
    sort_8_block(17, dest_blk, &buf);
    sort_8_block(17 + 8, dest_blk + 8, &buf);
    sort_8_block(17 + 16, dest_blk + 16, &buf);
    sort_8_block(17 + 24, dest_blk + 24, &buf);
    merge_groups(dest_blk, 4, 216, &buf);

    showBlocks(200, 48, &buf);
    int num_index_blocks;
    num_index_blocks = makeIndex(200, 16, 300, &buf);
    showIndex(300, num_index_blocks, &buf);
    int start_blk;
    start_blk = locateBlkbyIndex(30, 300, num_index_blocks, &buf);
    searchFromBlock(30, start_blk, &buf);
    return 0;
}