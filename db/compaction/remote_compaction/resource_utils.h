#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>

typedef struct MEMPACKED {
  char name1[20];
  unsigned long MemTotal;
  char name2[20];
  unsigned long MemFree;
  char name3[20];
  unsigned long Buffers;
  char name4[20];
  unsigned long Cached;
  char name5[20];
  unsigned long SwapCached;
} MEM_OCCUPY;

double get_memoccupy() {
  FILE *fd;
  char buff[256];
  MEM_OCCUPY *m;

  fd = fopen("/proc/meminfo", "r");
  fgets(buff, sizeof(buff), fd);
  sscanf(buff, "%s %lu ", m->name1, &m->MemTotal);
  fgets(buff, sizeof(buff), fd);
  sscanf(buff, "%s %lu ", m->name2, &m->MemFree);
  fgets(buff, sizeof(buff), fd);
  sscanf(buff, "%s %lu ", m->name3, &m->Buffers);
  fgets(buff, sizeof(buff), fd);
  sscanf(buff, "%s %lu ", m->name4, &m->Cached);
  fgets(buff, sizeof(buff), fd);
  sscanf(buff, "%s %lu", m->name5, &m->SwapCached);

  fclose(fd);
  return static_cast<double>(m->MemFree) / static_cast<double>(m->MemTotal);
}