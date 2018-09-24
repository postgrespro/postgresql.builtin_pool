/*-------------------------------------------------------------------------
 *
 * snapfs.h
 *	  File system snapshots providing fast recovery
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/snapfs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPFS_H
#define SNAPFS_H

#include "pg_config.h"
#include "port/atomics.h"
#include "catalog/pg_control.h"

#define SFS_INVALID_SNAPSHOT (0)

/*
 * Create new snapshot. Starting from this moment Postgres will store original copies of all updated pages.
 * Them will be stored in shanpshot file (*.snap.SNAP_ID and addressed through *.snapmap.SNAP_ID file) until
 * snapshot is deleted by sfs_remove_snapshot function or new snapshot is created.
 */
extern SnapshotId sfs_make_snapshot(void);

/*
 * Remove snapshot with all it's files
 */
extern void sfs_remove_snapshot(SnapshotId sid);

/*
 * Reset database state to the paritcular snapshot. It will be not possible any more to recover to any of more recent snashots or to the most recent database state.
 */
extern void sfs_recover_to_snapshot(SnapshotId sid);

/*
 * Temporary switch instance to the particular snashot. It will be possible to return back to the most recent database state or to switch to any other snapshot
 */
extern void sfs_switch_to_snapshot(SnapshotId sid);

/*
 * Set snapshot for backend,  unlike sfs_switch_to_snapshot function, it switchces snapshot for the current backend and not for all server instance.
 */
extern void sfs_set_backend_snapshot(SnapshotId sid);

/*
 * Get snapshot size (number of modified pages)
 */
extern int64 sfs_get_snapshot_size(SnapshotId sid);

/*
 * Get snapshot timestamp (time of first database modification in this snapshot)
 */
extern time_t sfs_get_snapshot_timestamp(SnapshotId sid);

/*
 * Iternal functions and variables
 */

#define SFS_KEEPING_SNAPSHOT() (ControlFile->recent_snapshot >= ControlFile->oldest_snapshot)
#define SFS_IN_SNAPSHOT()      (sfs_backend_snapshot != SFS_INVALID_SNAPSHOT || ControlFile->active_snapshot != SFS_INVALID_SNAPSHOT)

typedef uint32 sfs_segment_offs_t; /* segment size can not be greateer than 1Gb, so 4 bytes is enough */


typedef struct SnapshotMap
{
	pg_atomic_uint32   size; /* number of pages in the snapshot */
	sfs_segment_offs_t offs[RELSEG_SIZE]; /* offset of original page in snapshot file + 1 */
} SnapshotMap;

extern SnapshotId sfs_backend_snapshot;
extern SnapshotId sfs_active_snapshot;

/* Memory mapping function */
extern int sfs_munmap(SnapshotMap * map);
extern SnapshotMap * sfs_mmap(int md);
extern int sfs_msync(SnapshotMap * map);

/* Safe file IO functions */
extern int  sfs_read_file(int fd, void *data, uint32 size);
extern bool sfs_write_file(int fd, void const *data, uint32 size);

/* Provide exclusive access to databae */
extern void sfs_lock_database(void);
extern void sfs_unlock_database(void);

extern void sfs_check_snapshot(SnapshotId sid);

extern struct ControlFileData *ControlFile;

#endif
