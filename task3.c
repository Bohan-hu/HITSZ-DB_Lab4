#include "Tuple.h"
#include "extmem.h"
#include <stdio.h>
#include <stdlib.h>

int cmpTule(const void *p1, const void *p2) {
    Tuple t1 = *(Tuple *) p1;
    Tuple t2 = *(Tuple *) p2;
    return t1.a - t2.a;
}

Tuple getNextElement(int *blk_num, int *next_pos, unsigned char **blk, Buffer *buf, int end_blk_num) {   // Iterator
    if (*blk_num >= end_blk_num) {
        Tuple t;
        t.a = 9999999;
        return t;
    }
    if (*next_pos < 6) {
        *next_pos = *next_pos + 1;
        return getTuple_t(*blk, *next_pos - 1);
    } else {
        Tuple ret = getTuple_t(*blk, *next_pos);
        *next_pos = 0;
        *blk_num = *blk_num + 1;
        freeBlockInBuffer(*blk, buf);
        *blk = *blk_num < end_blk_num ? readBlockFromDisk(*blk_num, buf) : readBlockFromDisk(*blk_num - 1, buf);
        convert_blk_2int((Tuple *) *blk, 7);
        return ret;
    }
}

void output_Tuple(Tuple t, int *output_blk_num, int *next_pos, Tuple **blk, Buffer *buf, int flush, int isIndex) {
    int UPPER = isIndex ? 7 : 6;
    if (*blk == NULL && flush) {
        return;
    }
    if (*blk == NULL && !flush) {
        *blk = (Tuple *) getNewBlockInBuffer(buf);
    }
    if (*next_pos < UPPER && !flush) {
        (*blk)[*next_pos] = t;
        *next_pos = *next_pos + 1;
    } else if (*next_pos == UPPER) {
        (*blk)[*next_pos] = t;
        *next_pos = 0;
        if (!isIndex) {
            (*blk)[7].a = *output_blk_num + 1;
        }
        for (int offset = 0; offset < 8; offset++) {
            setTuple_str((unsigned char *) (*blk), offset,
                         getTuple_t((unsigned char *) (*blk), offset));
        }
        writeBlockToDisk((unsigned char **) blk, *output_blk_num, buf);
        *output_blk_num = *output_blk_num + 1;
    } else if (flush) {
        if (*next_pos == 0) return;
        *next_pos = 0;
        if (isIndex) {
            (*blk)[7].a = *output_blk_num + 1;
        }
        for (int offset = 0; offset < 8; offset++) {
            setTuple_str((unsigned char *) (*blk), offset,
                         getTuple_t((unsigned char *) (*blk), offset));
        }
        writeBlockToDisk((unsigned char **) blk, *output_blk_num, buf);
        *output_blk_num = *output_blk_num + 1;
    }
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
        convert_blk_2int((Tuple *) blk[i], 7);
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

int merge_groups(int start_block_num, int num_groups, int dst_start_block_num, Buffer *buf, int deduplicate) {
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
    int output_blk_cnt = dst_start_block_num;
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
            return -1;
        }
        convert_blk_2int((Tuple *) subset_buffers_ptr[i], 7);
        compare_buffer[i] = getTuple_t(subset_buffers_ptr[i], subset_buffer_read_pos[i]++);
    }
    int last_blk_key;

    while (1) {
        // Find the min pos
        int min_pos = 0;
        for (int i = 0; i < num_groups; i++) {
            if (compare_buffer[i].a < compare_buffer[min_pos].a) {
                min_pos = i;
            }
        }
        if (compare_buffer[min_pos].a == 9999999) {    // Reach the end....
            break;
        }
        if (deduplicate) {
            int flag = 0;
            if (compare_buffer[min_pos].a == last_blk_key && output_buffer_cnt == 0) {
                flag = 1;
            }
            // search the output buffer for the same element
            for (int k = 0; k < output_buffer_cnt; k++) {
                if (output_buffer[k].a == compare_buffer[min_pos].a) {
                    flag = 1;
                    break;
                }
            }
            if (!flag) {
                Tuple output_t;
                output_t.a = compare_buffer[min_pos].a;
                output_t.b = 0;
                output_Tuple(output_t, &output_blk_cnt, &output_buffer_cnt, &output_buffer, buf, 0, 0);
                if (output_buffer_cnt == 0) {
                    last_blk_key = output_t.a;
                }
            }
        } else {
            output_Tuple(compare_buffer[min_pos], &output_blk_cnt, &output_buffer_cnt, &output_buffer, buf, 0, 0);
        }
        // Send a new element to compare buffer
        // Reach the end of the block?
        compare_buffer[min_pos] = getNextElement(&(group_read_blk_num[min_pos]), &(subset_buffer_read_pos[min_pos]),
                                                 &(subset_buffers_ptr[min_pos]), buf,
                                                 group_start_block_num[min_pos + 1]);
    }
    // Do the cleanup
    for (int i = 0; i < num_groups; i++) {
        freeBlockInBuffer((unsigned char *) subset_buffers_ptr[i], buf);
    }
    Tuple t;
    output_Tuple(t, &output_blk_cnt, &output_buffer_cnt, &output_buffer, buf, 1, 0);

    freeBlockInBuffer((unsigned char *) compare_buffer, buf);
    return output_blk_cnt - dst_start_block_num;
}


int
join_intersect(int R_Start, int num_blks_R, int S_Start, int num_blks_S, int dest_blk_num, Buffer *buf, int intersect) {
    Tuple *RBuf = (Tuple *) readBlockFromDisk(R_Start, buf);
    Tuple *SBuf;
    Tuple *output_buffer = (Tuple *) getNewBlockInBuffer(buf);
    int R_cur_blk_num = R_Start;
    int output_buf_cnt = 0;
    // Read the block into the buffer
    Tuple S_Tup_bk;
    Tuple R_Tup_bk;
    int R_pos_bk;
    int R_blk_bk;
    for (int i = 0; i < num_blks_S; i++) {
        SBuf = (Tuple *) readBlockFromDisk(S_Start + i, buf);
        convert_blk_2int(SBuf, 7);
        // find the first place where S_i == R_i
        int R_next_pos = 0;
        int S_pos = 0;
        Tuple R_Tup = getNextElement(&R_cur_blk_num, &R_next_pos, (unsigned char **) (&RBuf), buf,
                                     R_Start + num_blks_R);
        Tuple S_Tup = SBuf[S_pos];
        if (i > 0 && S_Tup_bk.a == S_Tup.a) {
            freeBlockInBuffer((unsigned char *) RBuf, buf);
            R_cur_blk_num = R_blk_bk;
            R_next_pos = R_pos_bk;
            RBuf = (Tuple *) readBlockFromDisk(R_cur_blk_num, buf);
            R_Tup = R_Tup_bk;
        }
        while (S_pos < 7) {
            while (S_Tup.a != R_Tup.a) {
                if (S_Tup.a < R_Tup.a) {
                    S_pos++;
                    if (S_pos == 7) {
                        break;
                    }
                    S_Tup = SBuf[S_pos];
                } else {
                    if (R_next_pos == 0 && R_cur_blk_num > R_Start + num_blks_R) {
                        break;
                    }
                    R_Tup = getNextElement(&R_cur_blk_num, &R_next_pos, (unsigned char **) (&RBuf), buf,
                                           R_Start + num_blks_R);
                }
            }

            R_pos_bk = R_next_pos;
            R_blk_bk = R_cur_blk_num;
            R_Tup_bk = R_Tup;
            // find all the joinable numbers
            while (R_Tup.a == S_Tup.a) {
                if (intersect == 1) {
                    if (R_Tup.b == S_Tup.b) {
                        // TODO:Join a and b
                        printf("Intersect S(%d, %d) R(%d,%d)\n", S_Tup.a, S_Tup.b, R_Tup.a, R_Tup.b);
                    }
                } else {
                    // TODO:Join a and b
                    printf("Join S(%d, %d) R(%d,%d)\n", S_Tup.a, S_Tup.b, R_Tup.a, R_Tup.b);
                }
                // locate the next R
                R_Tup = getNextElement(&R_cur_blk_num, &R_next_pos, (unsigned char **) (&RBuf), buf,
                                       R_Start + num_blks_R);
            }
            S_pos++;
            if (S_pos == 7) {
                // Record the former S_pos
                S_Tup_bk = S_Tup;
                break;
            }
            S_Tup = SBuf[S_pos];
            // 如果相等，则需要撤回R的指针；如果不相等，则不需要
            if (S_Tup.a == SBuf[S_pos - 1].a) {
                freeBlockInBuffer((unsigned char *) RBuf, buf);
                R_cur_blk_num = R_blk_bk;
                R_next_pos = R_pos_bk;
                RBuf = (Tuple *) readBlockFromDisk(R_cur_blk_num, buf);
                R_Tup = R_Tup_bk;
            }
        }
        freeBlockInBuffer((unsigned char *) SBuf, buf);
    }
}

int makeIndex(int start_block, int num_blocks, int dst_block, Buffer *buf) {
    int blk_cnt;
    unsigned char *blk;
    Tuple *output_blk = (Tuple *) getNewBlockInBuffer(buf);
    int output_blk_pos = 0;
    int output_blk_cnt = dst_block;
    Tuple t;
    for (blk_cnt = start_block; blk_cnt < start_block + num_blocks; blk_cnt++) {
        blk = readBlockFromDisk(blk_cnt, buf);
        t.a = getTuple_str(blk, 0).a;
        t.b = blk_cnt;
        output_Tuple(t, &output_blk_cnt, &output_blk_pos, (Tuple **) &output_blk, buf, 0, 1);
        freeBlockInBuffer(blk, buf);
    }
    output_Tuple(t, &output_blk_cnt, &output_blk_pos, (Tuple **) &blk, buf, 1, 1);
    return output_blk_cnt - dst_block;
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
        freeBlockInBuffer(blk, buf);
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
        if (search_flag) {
            freeBlockInBuffer(blk, buf);
        }
    }
}

int main(int argc, char **argv) {
    Buffer buf; /* A buffer */
    /* Initialize the buffer */
    if (!initBuffer(520, 64, &buf)) { // Buffer size=520, blkSize=64
        perror("Buffer Initialization Failed!\n");
        return -1;
    }
    // Sort the block(8 blocks per group)
    int dest_blk = 100;
    sort_8_block(1, dest_blk, &buf);
    sort_8_block(1 + 8, dest_blk + 8, &buf);
    merge_groups(dest_blk, 2, 200, &buf, 0);
    dest_blk = 116;
    sort_8_block(17, dest_blk, &buf);
    sort_8_block(17 + 8, dest_blk + 8, &buf);
    sort_8_block(17 + 16, dest_blk + 16, &buf);
    sort_8_block(17 + 24, dest_blk + 24, &buf);
    merge_groups(dest_blk, 4, 216, &buf, 0);

//    showBlocks(200, 48, &buf);
//    int num_index_blocks;
//    num_index_blocks = makeIndex(200, 16, 300, &buf);
//    showBlocks(300, num_index_blocks, &buf);
//
//    int start_blk;
//    start_blk = locateBlkbyIndex(30, 300, num_index_blocks, &buf);
//    searchFromBlock(30, start_blk, &buf);
    int num_deduplicated_blocks;
    num_deduplicated_blocks = merge_groups(100, 2, 4000, &buf, 1);
    showBlocks(4000, num_deduplicated_blocks, &buf);
//    join_intersect(200, 16, 217, 32, 500, &buf, 1);
    return 0;
}