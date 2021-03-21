/*-------------------------------------------------------------------------
 *
 * toast_compression.c
 *	  Functions for toast compression.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/toast_compression.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#include "access/detoast.h"
#include "access/toast_compression.h"
#include "common/pg_lzcompress.h"
#include "fmgr.h"
#include "utils/builtins.h"

/* Compile-time default */
char	   *default_toast_compression = DEFAULT_TOAST_COMPRESSION;

/*
 * Compress a varlena using PGLZ.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
struct varlena *
pglz_compress_datum(const struct varlena *value)
{
	int32		valsize,
				len;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

	/*
	 * No point in wasting a palloc cycle if value size is outside the allowed
	 * range for compression.
	 */
	if (valsize < PGLZ_strategy_default->min_input_size ||
		valsize > PGLZ_strategy_default->max_input_size)
		return NULL;

	/*
	 * Figure out the maximum possible size of the pglz output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									VARHDRSZ_COMPRESS);

	len = pglz_compress(VARDATA_ANY(value),
						valsize,
						(char *) tmp + VARHDRSZ_COMPRESS,
						NULL);
	if (len < 0)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESS);

	return tmp;
}

/*
 * Decompress a varlena that was compressed using PGLZ.
 */
struct varlena *
pglz_decompress_datum(const struct varlena *value)
{
	struct varlena *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((char *) value + VARHDRSZ_COMPRESS,
							  VARSIZE(value) - VARHDRSZ_COMPRESS,
							  VARDATA(result),
							  VARRAWSIZE_4B_C(value), true);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Decompress part of a varlena that was compressed using PGLZ.
 */
struct varlena *
pglz_decompress_datum_slice(const struct varlena *value,
						int32 slicelength)
{
	struct varlena *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((char *) value + VARHDRSZ_COMPRESS,
							  VARSIZE(value) - VARHDRSZ_COMPRESS,
							  VARDATA(result),
							  slicelength, false);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Compress a varlena using LZ4.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
struct varlena *
lz4_compress_datum(const struct varlena *value)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
#else
	int32		valsize;
	int32		len;
	int32		max_size;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * Figure out the maximum possible size of the LZ4 output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	max_size = LZ4_compressBound(valsize);
	tmp = (struct varlena *) palloc(max_size + VARHDRSZ_COMPRESS);

	len = LZ4_compress_default(VARDATA_ANY(value),
							   (char *) tmp + VARHDRSZ_COMPRESS,
							   valsize, max_size);
	if (len <= 0)
		elog(ERROR, "lz4 compression failed");

	/* data is incompressible so just free the memory and return NULL */
	if (len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESS);

	return tmp;
#endif
}

/*
 * Decompress a varlena that was compressed using LZ4.
 */
struct varlena *
lz4_decompress_datum(const struct varlena *value)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
#else
	int32		rawsize;
	struct varlena *result;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(VARRAWSIZE_4B_C(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe((char *) value + VARHDRSZ_COMPRESS,
								  VARDATA(result),
								  VARSIZE(value) - VARHDRSZ_COMPRESS,
								  VARRAWSIZE_4B_C(value));
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));


	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Decompress part of a varlena that was compressed using LZ4.
 */
struct varlena *
lz4_decompress_datum_slice(const struct varlena *value, int32 slicelength)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
#else
	int32		rawsize;
	struct varlena *result;

	/* slice decompression not supported prior to 1.8.3 */
	if (LZ4_versionNumber() < 10803)
		return lz4_decompress_datum(value);

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe_partial((char *) value + VARHDRSZ_COMPRESS,
										  VARDATA(result),
										  VARSIZE(value) - VARHDRSZ_COMPRESS,
										  slicelength,
										  slicelength);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Extract compression ID from a varlena.
 *
 * Returns TOAST_INVALID_COMPRESSION_ID if the varlena is not compressed.
 */
ToastCompressionId
toast_get_compression_id(struct varlena *attr)
{
	ToastCompressionId	cmid = TOAST_INVALID_COMPRESSION_ID;

	/*
	 * If it is stored externally then fetch the compression method id from the
	 * external toast pointer.  If compressed inline, fetch it from the toast
	 * compression header.
	 */
	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			cmid = VARATT_EXTERNAL_GET_COMPRESSION(toast_pointer);
	}
	else if (VARATT_IS_COMPRESSED(attr))
		cmid = VARCOMPRESS_4B_C(attr);

	return cmid;
}

/*
 * Validate a new value for the default_toast_compression GUC.
 */
bool
check_default_toast_compression(char **newval, void **extra, GucSource source)
{
	if (**newval == '\0')
	{
		GUC_check_errdetail("%s cannot be empty.",
							"default_toast_compression");
		return false;
	}

	if (strlen(*newval) >= NAMEDATALEN)
	{
		GUC_check_errdetail("%s is too long (maximum %d characters).",
							"default_toast_compression", NAMEDATALEN - 1);
		return false;
	}

	if (!CompressionMethodIsValid(CompressionNameToMethod(*newval)))
	{
		/*
		 * When source == PGC_S_TEST, don't throw a hard error for a
		 * nonexistent compression method, only a NOTICE. See comments in
		 * guc.h.
		 */
		if (source == PGC_S_TEST)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("compression method \"%s\" does not exist",
							*newval)));
		}
		else
		{
			GUC_check_errdetail("Compression method \"%s\" does not exist.",
								*newval);
			return false;
		}
	}

	return true;
}
