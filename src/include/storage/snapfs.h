typedef int sfs_snapshot_id;
#define SFS_INVALID_SNAPSHOT (-1)

/*
 * Interface functions
 */

extern size_t sfs_shmem_size(void);
extern void sfs_initialize(void);
extern sfs_snapshot_id sfs_make_snapshot(void);
extern void sfs_remove_snapshot(sfs_snaphot_id sid);
extern void sfs_recover_to_snapshot(sfs_snaphot_id sid);
extern void sfs_set_backend_snapshot(sfs_snaphot_id sid);

/*
 * Iternal functions and variables
 */

typedef struct SfsState
{
	sfs_snapshot_id oldest_snapshot;
    sfs_snapshot_id recent_snapshot;
} SfsState;

typedef uint4 sfs_segment_offs_t;

typedef struct SnapshotMap
{
	pg_atomic_uint32   size;
	sfs_segment_offs_t offs[RELSEG_SIZE];
} SnapshotMap;

extern SfsState * sfs_state;
extern sfs_snapshot_id sfs_current_snapshot;

extern int sfs_munmap(SnapshotMap * map);
extern SnapshotMap * sfs_mmap(int md);
extern int sfs_msync(SnapshotMap * map);
extern bool sfs_read_file(int fd, void *data, uint32 size, off_t pos);
extern bool sfs_write_file(int fd, void const *data, uint32 size, off_t pos);
