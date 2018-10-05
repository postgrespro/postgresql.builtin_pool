#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"

#if HAVE_LIBZSTD

#include <malloc.h>
#include <zstd.h>

#define ZPQ_BUFFER_SIZE (8*1024)
#define ZSTD_COMPRESSION_LEVEL 1

struct ZpqStream
{
	ZSTD_CStream*  tx_stream;
	ZSTD_DStream*  rx_stream;
	ZSTD_outBuffer tx;
	ZSTD_inBuffer  rx;
	size_t         tx_not_flushed; /* Amount of datas in internal zstd buffer */
	size_t         tx_buffered;    /* Data which is consumed by zpq_read but not yet sent */
	zpq_tx_func    tx_func;
	zpq_rx_func    rx_func;
	void*          arg;
	char const*    rx_error;    /* Decompress error message */
	size_t         tx_total;
	size_t         tx_total_raw;
	size_t         rx_total;
	size_t         rx_total_raw;
	char           tx_buf[ZPQ_BUFFER_SIZE];
	char           rx_buf[ZPQ_BUFFER_SIZE];
};

ZpqStream*
zpq_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg)
{
	ZpqStream* zs = (ZpqStream*)malloc(sizeof(ZpqStream));
	zs->tx_stream = ZSTD_createCStream();
	ZSTD_initCStream(zs->tx_stream, ZSTD_COMPRESSION_LEVEL);
	zs->rx_stream = ZSTD_createDStream();
	ZSTD_initDStream(zs->rx_stream);
	zs->tx.dst = zs->tx_buf;
	zs->tx.pos = 0;
	zs->tx.size = ZPQ_BUFFER_SIZE;
	zs->rx.src = zs->rx_buf;
	zs->rx.pos = 0;
	zs->rx.size = 0;
	zs->rx_func = rx_func;
	zs->tx_func = tx_func;
	zs->tx_buffered = 0;
	zs->tx_not_flushed = 0;
	zs->rx_error = NULL;
	zs->arg = arg;
	zs->tx_total = zs->tx_total_raw = 0;
	zs->rx_total = zs->rx_total_raw = 0;
	return zs;
}

ssize_t
zpq_read(ZpqStream *zs, void *buf, size_t size, size_t *processed)
{
	ssize_t rc;
	ZSTD_outBuffer out;
	out.dst = buf;
	out.pos = 0;
	out.size = size;

	while (1)
	{
		rc = ZSTD_decompressStream(zs->rx_stream, &out, &zs->rx);
		if (ZSTD_isError(rc))
		{
			zs->rx_error = ZSTD_getErrorName(rc);
			return ZPQ_DECOMPRESS_ERROR;
		}
		/* Return result if we fill requested amount of bytes or read operation was performed */
		if (out.pos != 0)
		{
			zs->rx_total_raw += out.pos;
			return out.pos;
		}
		if (zs->rx.pos == zs->rx.size)
		{
			zs->rx.pos = zs->rx.size = 0; /* Reset rx buffer */
		}
		rc = zs->rx_func(zs->arg, (char*)zs->rx.src + zs->rx.size, ZPQ_BUFFER_SIZE - zs->rx.size);
		if (rc > 0) /* read fetches some data */
		{
			zs->rx.size += rc;
			zs->rx_total += rc;
		}
		else /* read failed */
		{
			*processed = out.pos;
			zs->rx_total_raw += out.pos;
			return rc;
		}
	}
}

ssize_t
zpq_write(ZpqStream *zs, void const *buf, size_t size, size_t *processed)
{
	ssize_t rc;
	ZSTD_inBuffer in_buf;
	in_buf.src = buf;
	in_buf.pos = 0;
	in_buf.size = size;

	do
	{
		if (zs->tx.pos == 0) /* Compress buffer is empty */
		{
			zs->tx.dst = zs->tx_buf; /* Reset pointer to the beginning of buffer */

			if (in_buf.pos < size) /* Has something to compress in input buffer */
				ZSTD_compressStream(zs->tx_stream, &zs->tx, &in_buf);

			if (in_buf.pos == size) /* All data is compressed: flushed internal zstd buffer */
			{
				zs->tx_not_flushed = ZSTD_flushStream(zs->tx_stream, &zs->tx);
			}
		}
		rc = zs->tx_func(zs->arg, zs->tx.dst, zs->tx.pos);
		if (rc > 0)
		{
			zs->tx.pos -= rc;
			zs->tx.dst = (char*)zs->tx.dst + rc;
			zs->tx_total += rc;
		}
		else
		{
			*processed = in_buf.pos;
			zs->tx_buffered = zs->tx.pos;
			zs->tx_total_raw += in_buf.pos;
			return rc;
		}
	} while (zs->tx.pos == 0 && (in_buf.pos < size || zs->tx_not_flushed)); /* repeat sending data until first partial write */

	zs->tx_total_raw += in_buf.pos;
	zs->tx_buffered = zs->tx.pos;
	return in_buf.pos;
}

void
zpq_free(ZpqStream *zs)
{
	if (zs != NULL)
	{
		ZSTD_freeCStream(zs->tx_stream);
		ZSTD_freeDStream(zs->rx_stream);
		free(zs);
	}
}

char const*
zpq_error(ZpqStream *zs)
{
	return zs->rx_error;
}

size_t
zpq_buffered(ZpqStream *zs)
{
	return zs != NULL ? zs->tx_buffered + zs->tx_not_flushed : 0;
}

char
zpq_algorithm(void)
{
	return 'f';
}

#elif HAVE_LIBZ

#include <malloc.h>
#include <zlib.h>

#define ZPQ_BUFFER_SIZE 8192
#define ZLIB_COMPRESSION_LEVEL 1

struct ZpqStream
{
	z_stream tx;
	z_stream rx;

	zpq_tx_func    tx_func;
	zpq_rx_func    rx_func;
	void*          arg;

	size_t         tx_buffered;

	Bytef          tx_buf[ZPQ_BUFFER_SIZE];
	Bytef          rx_buf[ZPQ_BUFFER_SIZE];
};

ZpqStream*
zpq_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg)
{
	int rc;
	ZpqStream* zs = (ZpqStream*)malloc(sizeof(ZpqStream));
	memset(&zs->tx, 0, sizeof(zs->tx));
	zs->tx.next_out = zs->tx_buf;
	zs->tx.avail_out = ZPQ_BUFFER_SIZE;
	zs->tx_buffered = 0;
	rc = deflateInit(&zs->tx, ZLIB_COMPRESSION_LEVEL);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->tx.next_out == zs->tx_buf && zs->tx.avail_out == ZPQ_BUFFER_SIZE);

	memset(&zs->rx, 0, sizeof(zs->tx));
	zs->rx.next_in = zs->rx_buf;
	zs->rx.avail_in = ZPQ_BUFFER_SIZE;
	rc = inflateInit(&zs->rx);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->rx.next_in == zs->rx_buf && zs->rx.avail_in == ZPQ_BUFFER_SIZE);
	zs->rx.avail_in = 0;

	zs->rx_func = rx_func;
	zs->tx_func = tx_func;
	zs->arg = arg;

	return zs;
}

ssize_t
zpq_read(ZpqStream *zs, void *buf, size_t size, size_t *processed)
{
	int rc;
	zs->rx.next_out = (Bytef *)buf;
	zs->rx.avail_out = size;

	while (1)
	{
		if (zs->rx.avail_in != 0) /* If there is some data in receiver buffer, then decompress it */
		{
			rc = inflate(&zs->rx, Z_SYNC_FLUSH);
			if (rc != Z_OK)
			{
				return ZPQ_DECOMPRESS_ERROR;
			}
			if (zs->rx.avail_out != size)
			{
				return size - zs->rx.avail_out;
			}
			if (zs->rx.avail_in == 0)
			{
				zs->rx.next_in = zs->rx_buf;
			}
		}
		else
		{
			zs->rx.next_in = zs->rx_buf;
		}
		rc = zs->rx_func(zs->arg, zs->rx.next_in + zs->rx.avail_in, zs->rx_buf + ZPQ_BUFFER_SIZE - zs->rx.next_in - zs->rx.avail_in);
		if (rc > 0)
		{
			zs->rx.avail_in += rc;
		}
		else
		{
			*processed = size - zs->rx.avail_out;
			return rc;
		}
	}
}

ssize_t
zpq_write(ZpqStream *zs, void const *buf, size_t size, size_t *processed)
{
    int rc;
	zs->tx.next_in = (Bytef *)buf;
	zs->tx.avail_in = size;
	do
	{
		if (zs->tx.avail_out == ZPQ_BUFFER_SIZE) /* Compress buffer is empty */
		{
			zs->tx.next_out = zs->tx_buf; /* Reset pointer to the  beginning of buffer */

			if (zs->tx.avail_in != 0) /* Has something in input buffer */
			{
				rc = deflate(&zs->tx, Z_SYNC_FLUSH);
				Assert(rc == Z_OK);
				zs->tx.next_out = zs->tx_buf; /* Reset pointer to the  beginning of buffer */
			}
		}
		rc = zs->tx_func(zs->arg, zs->tx.next_out, ZPQ_BUFFER_SIZE - zs->tx.avail_out);
		if (rc > 0)
		{
			zs->tx.next_out += rc;
			zs->tx.avail_out += rc;
		}
		else
		{
			*processed = size - zs->tx.avail_in;
			zs->tx_buffered = ZPQ_BUFFER_SIZE - zs->tx.avail_out;
			return rc;
		}
	} while (zs->tx.avail_out == ZPQ_BUFFER_SIZE && zs->tx.avail_in != 0); /* repeat sending data until first partial write */

	zs->tx_buffered = ZPQ_BUFFER_SIZE - zs->tx.avail_out;

	return size - zs->tx.avail_in;
}

void
zpq_free(ZpqStream *zs)
{
	if (zs != NULL)
	{
		inflateEnd(&zs->rx);
		deflateEnd(&zs->tx);
		free(zs);
	}
}

char const*
zpq_error(ZpqStream *zs)
{
	return zs->rx.msg;
}

size_t
zpq_buffered(ZpqStream *zs)
{
	return zs != NULL ? zs->tx_buffered : 0;
}

char
zpq_algorithm(void)
{
	return 'z';
}

#else

ZpqStream*
zpq_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg)
{
	return NULL;
}

ssize_t
zpq_read(ZpqStream *zs, void *buf, size_t size)
{
	return -1;
}

ssize_t
zpq_write(ZpqStream *zs, void const *buf, size_t size)
{
	return -1;
}

void
zpq_free(ZpqStream *zs)
{
}

char const*
zpq_error(ZpqStream *zs)
{
	return NULL;
}


size_t
zpq_buffered(ZpqStream *zs)
{
	return 0;
}

char
zpq_algorithm(void)
{
	return '0';
}

#endif
