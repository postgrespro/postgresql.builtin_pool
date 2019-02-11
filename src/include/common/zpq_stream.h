/*
 * zpq_stream.h
 *     Streaiming compression for libpq
 */

#ifndef ZPQ_STREAM_H
#define ZPQ_STREAM_H

#include <stdlib.h>

#define ZPQ_IO_ERROR (-1)
#define ZPQ_DECOMPRESS_ERROR (-2)
#define ZPQ_MAX_ALGORITHMS (8)
#define ZPQ_NO_COMPRESSION 'n'

struct ZpqStream;
typedef struct ZpqStream ZpqStream;

typedef ssize_t(*zpq_tx_func)(void* arg, void const* data, size_t size);
typedef ssize_t(*zpq_rx_func)(void* arg, void* data, size_t size);

ZpqStream* zpq_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void* arg);
ssize_t zpq_read(ZpqStream* zs, void* buf, size_t size, size_t* processed);
ssize_t zpq_write(ZpqStream* zs, void const* buf, size_t size, size_t* processed);
char const* zpq_error(ZpqStream* zs);
size_t zpq_buffered(ZpqStream* zs);
void zpq_free(ZpqStream* zs);

void zpq_get_supported_algorithms(char algorithms[ZPQ_MAX_ALGORITHMS]);
bool zpq_set_algorithm(char name);

#endif
