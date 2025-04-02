#include <assert.h>
#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include <string.h>

#define NUM_PROCS 4
#define CACHE_SIZE 4
#define MEM_SIZE 16
#define MSG_BUFFER_SIZE 256
#define MAX_INSTR_NUM 32

typedef unsigned char byte;

typedef enum
{
    MODIFIED,
    EXCLUSIVE,
    SHARED,
    INVALID
} cacheLineState;

typedef enum
{
    EM,
    S,
    U
} directoryEntryState;

typedef enum
{
    READ_REQUEST,
    WRITE_REQUEST,
    REPLY_RD,
    REPLY_WR,
    REPLY_ID,
    INV,
    UPGRADE,
    WRITEBACK_INV,
    WRITEBACK_INT,
    FLUSH,
    FLUSH_INVACK,
    EVICT_SHARED,
    EVICT_MODIFIED,
    INVACK
} transactionType;

typedef struct instruction
{
    byte type;
    byte address;
    byte value;
} instruction;

typedef struct cacheLine
{
    byte address;
    byte value;
    cacheLineState state;
} cacheLine;

typedef struct directoryEntry
{
    byte bitVector;
    directoryEntryState state;
} directoryEntry;

typedef struct message
{
    transactionType type;
    int sender;
    byte address;
    byte value;
    byte bitVector;
    int secondReceiver;
    directoryEntryState dirState;
} message;

typedef struct messageBuffer
{
    message queue[MSG_BUFFER_SIZE];
    int head;
    int tail;
    int count;
} messageBuffer;

typedef struct processorNode
{
    cacheLine cache[CACHE_SIZE];
    byte memory[MEM_SIZE];
    directoryEntry directory[MEM_SIZE];
    instruction instructions[MAX_INSTR_NUM];
    int instructionCount;
    int pendingInvAcks;
    byte pendingAddress;
    int pendingSender;
    byte pendingValue;
} processorNode;

void initializeProcessor(int threadId, processorNode *node, char *dirName);
void sendMessage(int receiver, message msg);
void handleCacheReplacement(int sender, cacheLine oldCacheLine);
void printProcessorState(int processorId, processorNode node);

messageBuffer messageBuffers[NUM_PROCS];
omp_lock_t msgBufferLocks[NUM_PROCS];

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <test_directory>\n", argv[0]);
        return EXIT_FAILURE;
    }
    char *dirName = argv[1];

    omp_set_num_threads(NUM_PROCS);

    for (int i = 0; i < NUM_PROCS; i++)
    {
        messageBuffers[i].count = 0;
        messageBuffers[i].head = 0;
        messageBuffers[i].tail = 0;
        omp_init_lock(&msgBufferLocks[i]);
    }

#pragma omp parallel
    {
        int threadId = omp_get_thread_num();
        processorNode node;
        initializeProcessor(threadId, &node, dirName);
        node.pendingInvAcks = 0;
        node.pendingAddress = 0xFF;
        node.pendingSender = -1;
        node.pendingValue = 0;

#pragma omp barrier

        message msg;
        instruction instr;
        int instructionIdx = -1;
        int printProcState = 1;
        byte waitingForReply = 0;

        while (1)
        {
            omp_set_lock(&msgBufferLocks[threadId]);
            while (messageBuffers[threadId].count > 0)
            {
                msg = messageBuffers[threadId].queue[messageBuffers[threadId].head];
                messageBuffers[threadId].head = (messageBuffers[threadId].head + 1) % MSG_BUFFER_SIZE;
                messageBuffers[threadId].count--;
                omp_unset_lock(&msgBufferLocks[threadId]);

                byte procNodeAddr = (msg.address >> 4) & 0x0F;
                byte memBlockAddr = msg.address & 0x0F;
                byte cacheIndex = memBlockAddr % CACHE_SIZE;

                switch (msg.type)
                {
                case READ_REQUEST:
                {
                    directoryEntry *dir = &node.directory[memBlockAddr];
                    if (dir->state == U)
                    {
                        message reply = {REPLY_RD, threadId, msg.address, node.memory[memBlockAddr], 0, -1, U};
                        dir->bitVector = (1 << msg.sender);
                        dir->state = EM;
                        sendMessage(msg.sender, reply);
                    }
                    else if (dir->state == S)
                    {
                        message reply = {REPLY_RD, threadId, msg.address, node.memory[memBlockAddr], dir->bitVector, -1, S};
                        sendMessage(msg.sender, reply);
                    }
                    else if (dir->state == EM)
                    {
                        int owner = __builtin_ffs(dir->bitVector) - 1;
                        message wb_msg = {WRITEBACK_INT, threadId, msg.address, 0, 0, msg.sender, EM};
                        sendMessage(owner, wb_msg);
                    }
                    break;
                }

                case REPLY_RD:
                {
                    cacheLine *cl = &node.cache[cacheIndex];
                    if (cl->address != 0xFF && cl->state != INVALID)
                        handleCacheReplacement(threadId, *cl);
                    cl->address = msg.address;
                    cl->value = msg.value;
                    cl->state = (msg.dirState == EM) ? EXCLUSIVE : SHARED;
                    waitingForReply = 0;
                    break;
                }

                case WRITEBACK_INT:
                {
                    cacheLine *cl = &node.cache[cacheIndex];
                    message flush = {FLUSH, threadId, msg.address, cl->value, 0, msg.secondReceiver, EM};
                    sendMessage(procNodeAddr, flush);
                    if (procNodeAddr != msg.secondReceiver)
                        sendMessage(msg.secondReceiver, flush);
                    cl->state = SHARED;
                    break;
                }

                case FLUSH:
                {
                    directoryEntry *dir = &node.directory[memBlockAddr];
                    if (threadId == procNodeAddr)
                    {
                        node.memory[memBlockAddr] = msg.value;
                        dir->bitVector = (1 << msg.sender);
                        dir->state = EM;
                        message reply = {REPLY_RD, threadId, msg.address, msg.value, 0, -1, EM};
                        sendMessage(msg.secondReceiver, reply);
                    }
                    else
                    {
                        cacheLine *cl = &node.cache[cacheIndex];
                        if (cl->address != 0xFF && cl->state != INVALID)
                            handleCacheReplacement(threadId, *cl);
                        cl->address = msg.address;
                        cl->value = msg.value;
                        cl->state = SHARED;
                    }
                    break;
                }

                case UPGRADE:
                {
                    directoryEntry *dir = &node.directory[memBlockAddr];
                    node.pendingAddress = msg.address;
                    node.pendingSender = msg.sender;
                    node.pendingInvAcks = 0;

                    for (int i = 0; i < NUM_PROCS; i++)
                    {
                        if ((dir->bitVector & (1 << i)) && (i != msg.sender))
                        {
                            message inv_msg = {INV, threadId, msg.address, 0, 0, -1, U};
                            sendMessage(i, inv_msg);
                            node.pendingInvAcks++;
                        }
                    }

                    if (node.pendingInvAcks == 0)
                    {
                        dir->bitVector = (1 << msg.sender);
                        dir->state = EM;
                        message reply = {REPLY_ID, threadId, msg.address, node.memory[memBlockAddr], dir->bitVector, -1, EM};
                        sendMessage(msg.sender, reply);
                    }
                    break;
                }

                case REPLY_ID:
                {
                    cacheLine *cl = &node.cache[cacheIndex];
                    if (cl->address != 0xFF && cl->state != INVALID)
                        handleCacheReplacement(threadId, *cl);
                    cl->address = msg.address;
                    cl->value = msg.value;
                    cl->state = MODIFIED;
                    waitingForReply = 0;
                    break;
                }

                case INV:
                {
                    cacheLine *cl = &node.cache[cacheIndex];
                    if (cl->address == msg.address)
                    {
                        cl->state = INVALID;
                        message ack = {INVACK, threadId, msg.address, 0, 0, msg.sender, U};
                        sendMessage(msg.sender, ack);
                    }
                    break;
                }

                case INVACK:
                {
                    node.pendingInvAcks--;
                    if (node.pendingInvAcks == 0 && node.pendingAddress != 0xFF)
                    {
                        directoryEntry *dir = &node.directory[node.pendingAddress & 0x0F];
                        dir->bitVector = (1 << node.pendingSender);
                        dir->state = EM;
                        node.memory[node.pendingAddress & 0x0F] = node.pendingValue;
                        message reply = {REPLY_WR, threadId, node.pendingAddress, node.pendingValue, 0, -1, EM};
                        sendMessage(node.pendingSender, reply);
                        node.pendingAddress = 0xFF;
                        node.pendingSender = -1;
                    }
                    break;
                }

                case WRITE_REQUEST:
                {
                    directoryEntry *dir = &node.directory[memBlockAddr];
                    if (dir->state == U)
                    {
                        dir->bitVector = (1 << msg.sender);
                        dir->state = EM;
                        node.memory[memBlockAddr] = msg.value;
                        message reply = {REPLY_WR, threadId, msg.address, msg.value, 0, -1, EM};
                        sendMessage(msg.sender, reply);
                    }
                    else if (dir->state == S)
                    {
                        node.pendingAddress = msg.address;
                        node.pendingSender = msg.sender;
                        node.pendingValue = msg.value;
                        node.pendingInvAcks = 0;

                        for (int i = 0; i < NUM_PROCS; i++)
                        {
                            if ((dir->bitVector & (1 << i)) && (i != msg.sender))
                            {
                                message inv_msg = {INV, threadId, msg.address, 0, 0, -1, U};
                                sendMessage(i, inv_msg);
                                node.pendingInvAcks++;
                            }
                        }

                        if (node.pendingInvAcks == 0)
                        {
                            dir->bitVector = (1 << msg.sender);
                            dir->state = EM;
                            node.memory[memBlockAddr] = msg.value;
                            message reply = {REPLY_WR, threadId, msg.address, msg.value, 0, -1, EM};
                            sendMessage(msg.sender, reply);
                        }
                    }
                    else if (dir->state == EM)
                    {
                        int owner = __builtin_ffs(dir->bitVector) - 1;
                        message wb_msg = {WRITEBACK_INV, threadId, msg.address, msg.value, 0, msg.sender, EM};
                        sendMessage(owner, wb_msg);
                    }
                    break;
                }

                case REPLY_WR:
                {
                    cacheLine *cl = &node.cache[cacheIndex];
                    if (cl->address != 0xFF && cl->state != INVALID)
                        handleCacheReplacement(threadId, *cl);
                    cl->address = msg.address;
                    cl->value = msg.value;
                    cl->state = MODIFIED;
                    waitingForReply = 0;
                    break;
                }

                case WRITEBACK_INV:
                {
                    cacheLine *cl = &node.cache[cacheIndex];
                    message flush = {FLUSH_INVACK, threadId, msg.address, cl->value, 0, msg.secondReceiver, EM};
                    sendMessage(procNodeAddr, flush);
                    if (procNodeAddr != msg.secondReceiver)
                        sendMessage(msg.secondReceiver, flush);
                    cl->state = INVALID;
                    break;
                }

                case FLUSH_INVACK:
                {
                    directoryEntry *dir = &node.directory[memBlockAddr];
                    if (threadId == procNodeAddr)
                    {
                        node.memory[memBlockAddr] = msg.value;
                        dir->bitVector = (1 << msg.secondReceiver);
                        dir->state = EM;
                        message reply = {REPLY_WR, threadId, msg.address, msg.value, 0, -1, EM};
                        sendMessage(msg.secondReceiver, reply);
                    }
                    else
                    {
                        cacheLine *cl = &node.cache[cacheIndex];
                        if (cl->address != 0xFF && cl->state != INVALID)
                            handleCacheReplacement(threadId, *cl);
                        cl->address = msg.address;
                        cl->value = msg.value;
                        cl->state = MODIFIED;
                    }
                    break;
                }

                case EVICT_SHARED:
                {
                    directoryEntry *dir = &node.directory[memBlockAddr];
                    dir->bitVector &= ~(1 << msg.sender);
                    if (dir->bitVector == 0)
                    {
                        dir->state = U;
                    }
                    else if ((dir->bitVector & (dir->bitVector - 1)) == 0)
                    {
                        dir->state = EM;
                        int owner = __builtin_ffs(dir->bitVector) - 1;
                        message evict_msg = {EVICT_SHARED, threadId, msg.address, 0, 0, owner, EM};
                        sendMessage(owner, evict_msg);
                    }
                    break;
                }

                case EVICT_MODIFIED:
                {
                    directoryEntry *dir = &node.directory[memBlockAddr];
                    node.memory[memBlockAddr] = msg.value;
                    dir->bitVector = 0;
                    dir->state = U;
                    break;
                }
                }
                omp_set_lock(&msgBufferLocks[threadId]);
            }
            omp_unset_lock(&msgBufferLocks[threadId]);

            if (waitingForReply || node.pendingInvAcks > 0)
                continue;

            if (instructionIdx < node.instructionCount - 1)
            {
                instructionIdx++;
            }
            else
            {
                if (printProcState > 0)
                {
                    printProcessorState(threadId, node);
                    printProcState--;
                }
                continue;
            }

            instr = node.instructions[instructionIdx];
            byte procNodeAddr = (instr.address >> 4) & 0x0F;
            byte memBlockAddr = instr.address & 0x0F;
            byte cacheIndex = memBlockAddr % CACHE_SIZE;
            cacheLine *cl = &node.cache[cacheIndex];

            if (instr.type == 'R')
            {
                if (cl->address != instr.address || cl->state == INVALID)
                {
                    message msg = {READ_REQUEST, threadId, instr.address, 0, 0, -1, U};
                    sendMessage(procNodeAddr, msg);
                    waitingForReply = 1;
                }
            }
            else
            {
                if (cl->address == instr.address && cl->state != INVALID)
                {
                    if (cl->state == SHARED)
                    {
                        message upgrade = {UPGRADE, threadId, instr.address, 0, 0, -1, EM};
                        sendMessage(procNodeAddr, upgrade);
                        waitingForReply = 1;
                    }
                    else
                    {
                        cl->value = instr.value;
                        if (cl->state == EXCLUSIVE)
                            cl->state = MODIFIED;
                    }
                }
                else
                {
                    message msg = {WRITE_REQUEST, threadId, instr.address, instr.value, 0, -1, U};
                    sendMessage(procNodeAddr, msg);
                    waitingForReply = 1;
                }
            }
        }
    }

    for (int i = 0; i < NUM_PROCS; i++)
        omp_destroy_lock(&msgBufferLocks[i]);
    return EXIT_SUCCESS;
}

void sendMessage(int receiver, message msg)
{
    omp_set_lock(&msgBufferLocks[receiver]);
    if (messageBuffers[receiver].count < MSG_BUFFER_SIZE)
    {
        messageBuffers[receiver].queue[messageBuffers[receiver].tail] = msg;
        messageBuffers[receiver].tail = (messageBuffers[receiver].tail + 1) % MSG_BUFFER_SIZE;
        messageBuffers[receiver].count++;
    }
    omp_unset_lock(&msgBufferLocks[receiver]);
}

void handleCacheReplacement(int sender, cacheLine oldCacheLine)
{
    byte procNodeAddr = (oldCacheLine.address >> 4) & 0x0F;
    byte memBlockAddr = oldCacheLine.address & 0x0F;
    if (oldCacheLine.state == MODIFIED)
    {
        message msg = {EVICT_MODIFIED, sender, oldCacheLine.address, oldCacheLine.value, 0, -1, EM};
        sendMessage(procNodeAddr, msg);
    }
    else if (oldCacheLine.state == SHARED || oldCacheLine.state == EXCLUSIVE)
    {
        message msg = {EVICT_SHARED, sender, oldCacheLine.address, 0, 0, -1, EM};
        sendMessage(procNodeAddr, msg);
    }
}

void initializeProcessor(int threadId, processorNode *node, char *dirName)
{
    // IMPORTANT: DO NOT MODIFY
    for (int i = 0; i < MEM_SIZE; i++)
    {
        node->memory[i] = 20 * threadId + i; // some initial value to mem block
        node->directory[i].bitVector = 0;    // no cache has this block at start
        node->directory[i].state = U;        // this block is in Unowned state
    }

    for (int i = 0; i < CACHE_SIZE; i++)
    {
        node->cache[i].address = 0xFF; // this address is invalid as we can
                                       // have a maximum of 8 nodes in the
                                       // current implementation
        node->cache[i].value = 0;
        node->cache[i].state = INVALID; // all cache lines are invalid
    }

    // read and parse instructions from core_<threadId>.txt
    char filename[128];
    snprintf(filename, sizeof(filename), "tests/%s/core_%d.txt", dirName, threadId);
    FILE *file = fopen(filename, "r");
    if (!file)
    {
        fprintf(stderr, "Error: count not open file %s\n", filename);
        exit(EXIT_FAILURE);
    }

    char line[20];
    node->instructionCount = 0;
    while (fgets(line, sizeof(line), file) &&
           node->instructionCount < MAX_INSTR_NUM)
    {
        if (line[0] == 'R' && line[1] == 'D')
        {
            sscanf(line, "RD %hhx",
                   &node->instructions[node->instructionCount].address);
            node->instructions[node->instructionCount].type = 'R';
            node->instructions[node->instructionCount].value = 0;
        }
        else if (line[0] == 'W' && line[1] == 'R')
        {
            sscanf(line, "WR %hhx %hhu",
                   &node->instructions[node->instructionCount].address,
                   &node->instructions[node->instructionCount].value);
            node->instructions[node->instructionCount].type = 'W';
        }
        node->instructionCount++;
    }

    fclose(file);
    printf("Processor %d initialized\n", threadId);
}

void printProcessorState(int processorId, processorNode node)
{
    // IMPORTANT: DO NOT MODIFY
    static const char *cacheStateStr[] = {"MODIFIED", "EXCLUSIVE", "SHARED",
                                          "INVALID"};
    static const char *dirStateStr[] = {"EM", "S", "U"};

    char filename[32];
    snprintf(filename, sizeof(filename), "core_%d_output.txt", processorId);

    FILE *file = fopen(filename, "w");
    if (!file)
    {
        printf("Error: Could not open file %s\n", filename);
        return;
    }

    fprintf(file, "=======================================\n");
    fprintf(file, " Processor Node: %d\n", processorId);
    fprintf(file, "=======================================\n\n");

    // Print memory state
    fprintf(file, "-------- Memory State --------\n");
    fprintf(file, "| Index | Address |   Value  |\n");
    fprintf(file, "|----------------------------|\n");
    for (int i = 0; i < MEM_SIZE; i++)
    {
        fprintf(file, "|  %3d  |  0x%02X   |  %5d   |\n", i, (processorId << 4) + i,
                node.memory[i]);
    }
    fprintf(file, "------------------------------\n\n");

    // Print directory state
    fprintf(file, "------------ Directory State ---------------\n");
    fprintf(file, "| Index | Address | State |    BitVector   |\n");
    fprintf(file, "|------------------------------------------|\n");
    for (int i = 0; i < MEM_SIZE; i++)
    {
        fprintf(file, "|  %3d  |  0x%02X   |  %2s   |   0x%08X   |\n",
                i, (processorId << 4) + i, dirStateStr[node.directory[i].state],
                node.directory[i].bitVector);
    }
    fprintf(file, "--------------------------------------------\n\n");

    // Print cache state
    fprintf(file, "------------ Cache State ----------------\n");
    fprintf(file, "| Index | Address | Value |    State    |\n");
    fprintf(file, "|---------------------------------------|\n");
    for (int i = 0; i < CACHE_SIZE; i++)
    {
        fprintf(file, "|  %3d  |  0x%02X   |  %3d  |  %8s \t|\n",
                i, node.cache[i].address, node.cache[i].value,
                cacheStateStr[node.cache[i].state]);
    }
    fprintf(file, "----------------------------------------\n\n");

    fclose(file);
}
