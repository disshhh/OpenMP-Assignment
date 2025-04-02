#include <assert.h>
#include <stdio.h>
#include <omp.h>
#include <stdlib.h>

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
    INVACK // Added this one new transaction type
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
    processorNode node;

#pragma omp parallel private(node)
    {
        int threadId = omp_get_thread_num();
        initializeProcessor(threadId, &node, dirName);
#pragma omp barrier

        message msg;
        message msgReply;
        instruction instr;
        int instructionIdx = -1;
        int printProcState = 1;
        byte waitingForReply = 0;
        while (1)
        {
            while (
                messageBuffers[threadId].count > 0 &&
                messageBuffers[threadId].head != messageBuffers[threadId].tail)
            {
                if (printProcState == 0)
                {
                    printProcState++;
                }
                int head = messageBuffers[threadId].head;
                msg = messageBuffers[threadId].queue[head];
                messageBuffers[threadId].head = (head + 1) % MSG_BUFFER_SIZE;

                byte procNodeAddr = (msg.address >> 4) & 0x0F;
                byte memBlockAddr = msg.address & 0x0F;
                byte cacheIndex = memBlockAddr % CACHE_SIZE;

                switch (msg.type)
                {
                case READ_REQUEST:
                    if (node.directory[memBlockAddr].state == U || node.directory[memBlockAddr].state == S)
                    {
                        msgReply.type = REPLY_RD;
                        msgReply.sender = threadId;
                        msgReply.address = msg.address;
                        msgReply.value = node.memory[memBlockAddr];
                        sendMessage(msg.sender, msgReply);

                        if (msg.sender == threadId && node.directory[memBlockAddr].state == U)
                        {
                            node.directory[memBlockAddr].state = EM;
                        }
                        else
                        {
                            node.directory[memBlockAddr].state = S;
                        }
                        node.directory[memBlockAddr].bitVector |= (1 << msg.sender);
                    }
                    else if (node.directory[memBlockAddr].state == EM)
                    {
                        msgReply.type = WRITEBACK_INT;
                        msgReply.sender = msg.sender;
                        msgReply.address = msg.address;

                        for (int i = 0; i < NUM_PROCS; i++)
                        {
                            if (node.directory[memBlockAddr].bitVector & (1 << i))
                            {
                                sendMessage(i, msgReply);
                                break;
                            }
                        }
                    }
                    break;

                case REPLY_RD:
                    if (node.cache[cacheIndex].state != INVALID && node.cache[cacheIndex].address != msg.address)
                    {
                        handleCacheReplacement(threadId, node.cache[cacheIndex]);
                    }
                    node.cache[cacheIndex].address = msg.address;
                    node.cache[cacheIndex].value = msg.value;
                    if (msg.sender == threadId && node.directory[memBlockAddr].state == EM)
                    {
                        node.cache[cacheIndex].state = EXCLUSIVE;
                    }
                    else
                    {
                        node.cache[cacheIndex].state = SHARED;
                    }
                    waitingForReply = 0;
                    break;

                case WRITEBACK_INT:
                    msgReply.type = FLUSH;
                    msgReply.sender = threadId;
                    msgReply.address = msg.address;
                    msgReply.value = node.cache[cacheIndex].value;
                    sendMessage(procNodeAddr, msgReply);

                    if (procNodeAddr != msg.sender)
                    {
                        sendMessage(msg.sender, msgReply);
                    }

                    node.cache[cacheIndex].state = SHARED;
                    break;

                case FLUSH:
                    if (threadId == procNodeAddr)
                    {
                        node.memory[memBlockAddr] = msg.value;
                        node.directory[memBlockAddr].state = S;
                        node.directory[memBlockAddr].bitVector |= (1 << msg.sender);
                    }
                    else
                    {
                        if (node.cache[cacheIndex].state != INVALID && node.cache[cacheIndex].address != msg.address)
                        {
                            handleCacheReplacement(threadId, node.cache[cacheIndex]);
                        }
                        node.cache[cacheIndex].address = msg.address;
                        node.cache[cacheIndex].value = msg.value;
                        node.cache[cacheIndex].state = SHARED;
                    }
                    waitingForReply = 0;
                    break;

                case UPGRADE:
                    msgReply.type = REPLY_ID;
                    msgReply.sender = threadId;
                    msgReply.address = msg.address;
                    msgReply.bitVector = node.directory[memBlockAddr].bitVector & ~(1 << msg.sender);
                    msgReply.value = msg.value;
                    sendMessage(msg.sender, msgReply);

                    node.directory[memBlockAddr].state = EM;
                    node.directory[memBlockAddr].bitVector = (1 << msg.sender);
                    break;

                case REPLY_ID:
                    for (int i = 0; i < NUM_PROCS; i++)
                    {
                        if (msg.bitVector & (1 << i))
                        {
                            msgReply.type = INV;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;
                            sendMessage(i, msgReply);
                        }
                    }

                    if (node.cache[cacheIndex].state != INVALID && node.cache[cacheIndex].address != msg.address)
                    {
                        handleCacheReplacement(threadId, node.cache[cacheIndex]);
                    }

                    node.cache[cacheIndex].address = msg.address;
                    if (instr.type == 'W')
                    {
                        node.cache[cacheIndex].value = instr.value;
                    }
                    else
                    {
                        node.cache[cacheIndex].value = msg.value;
                    }
                    node.cache[cacheIndex].state = MODIFIED;
                    waitingForReply = 0;
                    break;

                case INV:
                    if (node.cache[cacheIndex].address == msg.address)
                    {
                        node.cache[cacheIndex].state = INVALID;
                        // Added these 3 lines to send INVACK
                        msgReply.type = INVACK;
                        msgReply.sender = threadId;
                        msgReply.address = msg.address;
                        sendMessage(msg.sender, msgReply);
                    }
                    break;

                // Added this new case for INVACK
                case INVACK:
                    // Simple acknowledgment - no state change needed
                    break;

                case WRITE_REQUEST:
                    if (node.directory[memBlockAddr].state == U)
                    {
                        msgReply.type = REPLY_WR;
                        msgReply.sender = threadId;
                        msgReply.address = msg.address;
                        msgReply.value = node.memory[memBlockAddr];
                        sendMessage(msg.sender, msgReply);

                        node.directory[memBlockAddr].state = EM;
                        node.directory[memBlockAddr].bitVector = (1 << msg.sender);
                    }
                    else if (node.directory[memBlockAddr].state == S)
                    {
                        msgReply.type = REPLY_ID;
                        msgReply.sender = threadId;
                        msgReply.address = msg.address;
                        msgReply.bitVector = node.directory[memBlockAddr].bitVector;
                        msgReply.value = node.memory[memBlockAddr];
                        sendMessage(msg.sender, msgReply);

                        node.directory[memBlockAddr].state = EM;
                        node.directory[memBlockAddr].bitVector = (1 << msg.sender);
                    }
                    else if (node.directory[memBlockAddr].state == EM)
                    {
                        msgReply.type = WRITEBACK_INV;
                        msgReply.sender = msg.sender;
                        msgReply.address = msg.address;

                        for (int i = 0; i < NUM_PROCS; i++)
                        {
                            if (node.directory[memBlockAddr].bitVector & (1 << i))
                            {
                                sendMessage(i, msgReply);
                                break;
                            }
                        }
                    }
                    break;

                case REPLY_WR:
                    if (node.cache[cacheIndex].state != INVALID && node.cache[cacheIndex].address != msg.address)
                    {
                        handleCacheReplacement(threadId, node.cache[cacheIndex]);
                    }

                    node.cache[cacheIndex].address = msg.address;
                    node.cache[cacheIndex].value = instr.value;
                    node.cache[cacheIndex].state = MODIFIED;
                    waitingForReply = 0;
                    break;

                case WRITEBACK_INV:
                    msgReply.type = FLUSH_INVACK;
                    msgReply.sender = msg.sender;
                    msgReply.address = msg.address;
                    msgReply.value = node.cache[cacheIndex].value;
                    sendMessage(procNodeAddr, msgReply);

                    if (procNodeAddr != msg.sender)
                    {
                        sendMessage(msg.sender, msgReply);
                    }

                    node.cache[cacheIndex].state = INVALID;
                    break;

                case FLUSH_INVACK:
                    if (threadId == procNodeAddr)
                    {
                        node.memory[memBlockAddr] = msg.value;
                        node.directory[memBlockAddr].state = EM;
                        node.directory[memBlockAddr].bitVector = (1 << msg.sender);
                    }
                    else
                    {
                        if (node.cache[cacheIndex].state != INVALID && node.cache[cacheIndex].address != msg.address)
                        {
                            handleCacheReplacement(threadId, node.cache[cacheIndex]);
                        }
                        node.cache[cacheIndex].address = msg.address;
                        node.cache[cacheIndex].value = instr.value;
                        node.cache[cacheIndex].state = MODIFIED;
                    }
                    waitingForReply = 0;
                    break;

                case EVICT_SHARED:
                    node.directory[memBlockAddr].bitVector &= ~(1 << msg.sender);

                    if (node.directory[memBlockAddr].bitVector == 0)
                    {
                        node.directory[memBlockAddr].state = U;
                    }
                    else if (__builtin_popcount(node.directory[memBlockAddr].bitVector) == 1)
                    {
                        node.directory[memBlockAddr].state = EM;

                        for (int i = 0; i < NUM_PROCS; i++)
                        {
                            if (node.directory[memBlockAddr].bitVector & (1 << i))
                            {
                                msgReply.type = EVICT_SHARED;
                                msgReply.sender = threadId;
                                msgReply.address = msg.address;
                                sendMessage(i, msgReply);
                                break;
                            }
                        }
                    }
                    break;

                case EVICT_MODIFIED:
                    node.memory[memBlockAddr] = msg.value;
                    node.directory[memBlockAddr].state = U;
                    node.directory[memBlockAddr].bitVector = 0;
                    break;
                }
            }

            if (waitingForReply > 0)
            {
                continue;
            }

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

            if (instr.type == 'R')
            {
                if (node.cache[cacheIndex].address == instr.address && node.cache[cacheIndex].state != INVALID)
                {
                    instr.value = node.cache[cacheIndex].value;
                }
                else
                {
                    msg.type = READ_REQUEST;
                    msg.sender = threadId;
                    msg.address = instr.address;
                    sendMessage(procNodeAddr, msg);
                    waitingForReply = 1;
                }
            }
            else
            {
                if (node.cache[cacheIndex].address == instr.address && node.cache[cacheIndex].state != INVALID)
                {
                    if (node.cache[cacheIndex].state == MODIFIED || node.cache[cacheIndex].state == EXCLUSIVE)
                    {
                        node.cache[cacheIndex].value = instr.value;
                        node.cache[cacheIndex].state = MODIFIED;
                    }
                    else if (node.cache[cacheIndex].state == SHARED)
                    {
                        msg.type = UPGRADE;
                        msg.sender = threadId;
                        msg.address = instr.address;
                        msg.value = instr.value;
                        sendMessage(procNodeAddr, msg);
                        waitingForReply = 1;
                    }
                }
                else
                {
                    msg.type = WRITE_REQUEST;
                    msg.sender = threadId;
                    msg.address = instr.address;
                    msg.value = instr.value;
                    sendMessage(procNodeAddr, msg);
                    waitingForReply = 1;
                }
            }
        }
    }
}

void sendMessage(int receiver, message msg)
{
    omp_set_lock(&msgBufferLocks[receiver]);
    messageBuffers[receiver].queue[messageBuffers[receiver].tail] = msg;
    messageBuffers[receiver].tail = (messageBuffers[receiver].tail + 1) % MSG_BUFFER_SIZE;
    messageBuffers[receiver].count++;
    omp_unset_lock(&msgBufferLocks[receiver]);
}

void handleCacheReplacement(int sender, cacheLine oldCacheLine)
{
    byte memBlockAddr = oldCacheLine.address & 0x0F;
    byte procNodeAddr = (oldCacheLine.address >> 4) & 0x0F;
    message msg;

    switch (oldCacheLine.state)
    {
    case EXCLUSIVE:
    case SHARED:
        msg.type = EVICT_SHARED;
        msg.sender = sender;
        msg.address = oldCacheLine.address;
        sendMessage(procNodeAddr, msg);
        break;
    case MODIFIED:
        msg.type = EVICT_MODIFIED;
        msg.sender = sender;
        msg.address = oldCacheLine.address;
        msg.value = oldCacheLine.value;
        sendMessage(procNodeAddr, msg);
        break;
    case INVALID:
        break;
    }
}

void initializeProcessor(int threadId, processorNode *node, char *dirName)
{
    for (int i = 0; i < MEM_SIZE; i++)
    {
        node->memory[i] = 20 * threadId + i;
        node->directory[i].bitVector = 0;
        node->directory[i].state = U;
    }

    for (int i = 0; i < CACHE_SIZE; i++)
    {
        node->cache[i].address = 0xFF;
        node->cache[i].value = 0;
        node->cache[i].state = INVALID;
    }

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
    static const char *cacheStateStr[] = {"MODIFIED", "EXCLUSIVE", "SHARED", "INVALID"};
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

    fprintf(file, "-------- Memory State --------\n");
    fprintf(file, "| Index | Address |   Value  |\n");
    fprintf(file, "|----------------------------|\n");
    for (int i = 0; i < MEM_SIZE; i++)
    {
        fprintf(file, "|  %3d  |  0x%02X   |  %5d   |\n",
                i, (processorId << 4) + i, node.memory[i]);
    }
    fprintf(file, "------------------------------\n\n");

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
