# Archive Backend

A storage backend that keeps the data folder on the local disk, mirrors it to a
second directory (the archive, typically a NAS share mounted over NFS or SMB) and
optionally keeps only the most recently used part of the data local.

It gives two things with one mechanism:

- **Backup**: every file of the data folder is copied to the archive shortly after
  it stops changing. The archive is a complete, loadable ReductStore data folder.
- **Tiering**: when `RS_ARCHIVE_LOCAL_SIZE` is set, the coldest data blocks that are
  already in the archive are removed from the local disk. They stay visible in
  every bucket and are fetched back transparently when they are read.

Clients see one instance with all the data. Nothing changes in the HTTP or Zenoh
API except that `GET /api/v1/info` reports the state of the archive.

## Setup

1. Create a share on the NAS and mount it on the server.

   NFS is preferred: it is case-sensitive like the entry names and has the rename
   and sync behaviour the copies rely on. Use a soft mount so that a NAS outage
   turns into errors instead of hanging threads:

   ```text
   nas.local:/var/nfs/shared/reductstore  /mnt/reductstore-archive  nfs  soft,timeo=150,retrans=3,noatime,_netdev,nofail,x-systemd.automount  0 0
   ```

   With Docker, mount the share on the host and bind it into the container
   (`-v /mnt/reductstore-archive:/archive:rslave`), so that the container starts
   whether or not the NAS is reachable.

2. Create the marker file once, on the mounted share:

   ```sh
   touch /mnt/reductstore-archive/.reductstore-archive
   ```

   The server never creates this file. An unmounted share is an empty directory,
   and without the marker it would be indistinguishable from a new archive: the
   server would start filling the disk under the mount point. The marker also
   records which data folder the archive belongs to.

3. Start the server with the archive enabled:

   ```sh
   RS_ARCHIVE_PATH=/mnt/reductstore-archive RS_ARCHIVE_LOCAL_SIZE=500GB reductstore
   ```

The first start copies the whole data folder, which takes as long as the network
and the NAS allow. Eviction follows the copies, so the local usage shrinks to the
limit as the backlog drains.

## Settings

| Variable                   | Default | Meaning                                                                                              |
|----------------------------|---------|------------------------------------------------------------------------------------------------------|
| `RS_ARCHIVE_PATH`          | unset   | Directory of the archive. Enables the backend.                                                       |
| `RS_ARCHIVE_LOCAL_SIZE`    | unset   | Maximum size of the local data folder (`500GB`). Unset keeps everything local: backup only.          |
| `RS_ARCHIVE_SYNC_DELAY`    | `30`    | Seconds a file must stay unchanged before it is copied.                                              |
| `RS_ARCHIVE_MAX_LAG`       | `600`   | Seconds after which a file that keeps changing is copied anyway. Bounds the data lost with the disk. |
| `RS_ARCHIVE_MIN_IDLE`      | `300`   | Seconds a block must stay unwritten and unread before it can be evicted.                             |
| `RS_ARCHIVE_VERIFY`        | `true`  | Read a block back from the archive and compare it with the local one before evicting it.             |
| `RS_ARCHIVE_IO_TIMEOUT`    | `120`   | Seconds a single archive operation may take before the archive is considered offline.                |
| `RS_ARCHIVE_SCAN_INTERVAL` | `300`   | Seconds between consistency scans of the local data folder.                                          |
| `RS_ARCHIVE_ADOPT`         | `false` | Attach a data folder without archive identity to an archive that already has one (restore).          |

The bucket quota keeps its meaning: it limits the size of a bucket wherever its
blocks are. A FIFO quota is therefore the total retention, and blocks it removes
are removed from the archive as well. `RS_ARCHIVE_LOCAL_SIZE` only decides how much
of that is local.

A `compress` lifecycle policy has to read a block to compress it. If its
`older_than` is longer than the data stays local, every block is fetched back,
compressed and copied again, so compress before the data is expected to leave.

The archive is ignored on replica instances.

## State

`GET /api/v1/info` contains an `archive` object when the backend is enabled:

| Field              | Meaning                                                              |
|--------------------|----------------------------------------------------------------------|
| `online`           | The archive is mounted, has the marker and belongs to this instance  |
| `ready`            | The local data folder is indexed; eviction waits for this            |
| `local_usage`      | Bytes on the local disk                                              |
| `local_size_limit` | `RS_ARCHIVE_LOCAL_SIZE`                                              |
| `archived_usage`   | Bytes of the blocks that are only in the archive                     |
| `archived_files`   | Number of those blocks                                               |
| `pending_usage`    | Bytes waiting to be copied                                           |
| `pending_files`    | Files waiting to be copied                                           |
| `pending_removals` | Removals and renames waiting to be applied to the archive            |
| `lag`              | Seconds the oldest pending change has been waiting                   |
| `archive_free`     | Free space of the archive                                            |
| `archive_total`    | Capacity of the archive                                              |
| `last_sync`        | UNIX time of the last copy                                           |
| `recalled_files`   | Blocks fetched back since start                                      |
| `last_error`       | Last archive error                                                   |

## Behaviour During an Outage

The archive is never on the write path. While it is unreachable:

- writes, queries of local data, removals and renames work as usual;
- reading the content of an evicted block fails with `503 Service Unavailable`
  for that record only; metadata-only queries of archived data keep working;
- nothing is evicted, so the local folder grows past its limit until the archive
  is back, bounded by the free disk space check and the bucket quotas;
- removals and renames are written to `<data>/.archive/journal` and applied when
  the archive returns, also across restarts.

## Restore

The archive under `<archive>/data` is a plain data folder.

- Lost disk: mount the share, copy `<archive>/data/` to the new data folder with
  the modification times preserved (`rsync -a`), and start the server with
  `RS_ARCHIVE_ADOPT=true` once. Files whose size and time match the archive are
  not copied again.
- Read the archive without the original server: start a second instance with
  `RS_DATA_PATH` pointing at a copy of `<archive>/data`.

Snapshots on the NAS are what protect against removals made through the API: a
removed record is removed from the archive too.

## Layout

```text
<data>/                                 local data folder
  <bucket>/<entry>/<id>.blk             block that is local
  <bucket>/<entry>/<id>.blk.archived    stub of a block that is only in the archive
  .archive/instance                     identity shared with the archive marker
  .archive/journal                      removals and renames not yet applied

<archive>/
  .reductstore-archive                  marker, created by the operator
  data/<bucket>/<entry>/...             mirror of the data folder
  .tmp/                                 copies in progress
```

Only block data (`.blk`, `.blk.zst`) is evicted. Descriptors, block indexes,
settings, tokens, replication and lifecycle definitions stay local and are
mirrored. WAL segments, temporary files, replication logs and the lock file are
not mirrored.

## Design

The engine does all file I/O through the file cache, which calls a `StorageBackend`
at fixed points. The archive backend is an implementation of that trait and uses
the points like this:

| Hook                              | Called                                  | Archive backend                                    |
|-----------------------------------|-----------------------------------------|----------------------------------------------------|
| `upload`                          | when a file is synced or published      | marks the file as changed                          |
| `prepare_open`                    | before a file is opened                 | marks the block as used                            |
| `try_exists`, `get_stats`         | existence and size checks               | answers for evicted blocks from their stubs        |
| `read_dir`                        | listings, integrity check               | lists evicted blocks as if they were there         |
| `download`                        | a file that exists is not there locally | fetches the block back                             |
| `update_local_cache`              | every access of a cached descriptor     | marks the block as used, reports stale descriptors |
| `invalidate_locally_cached_files` | every 100 ms                            | returns the blocks that are verified for eviction  |
| `remove_from_local_cache_if_unused` | after the cache closed such a block   | replaces the block by its stub                     |
| `remove`, `remove_dir_all`        | removals                                | removes locally, writes the journal                |
| `rename`                          | entry and bucket renames                | writes the journal, renames locally                |

Files:

- `archive.rs`: the backend, eviction commit and recall
- `index.rs`: in-memory picture of the data folder used for the decisions
- `worker.rs`: the background cycle (probe, scan, journal, copy, evict)
- `remote.rs`: archive I/O with concurrency and time limits, marker and identity
- `journal.rs`: removals and renames waiting for the archive
- `stub.rs`: the stub format

### Source of truth

What the engine is told comes from the local filesystem only: a block exists if
its file or its stub exists. The index is a cache of that and of what is known
about the archive. If it is wrong, a copy or an eviction is delayed, nothing else.
This matters because the integrity check at startup removes the descriptor of
every block whose data file is not listed; with the listing depending on the
archive, a restart during an outage would drop everything that is archived.

### Copies

A file is copied when it has been unchanged for `sync_delay`, or when it has been
changing for `max_lag`. A copy goes to a temporary name on the archive, is synced
and renamed into place, and is thrown away when the source changed meanwhile, so
the archive only ever holds complete versions.

A descriptor is never copied before its data file. For a block that is still
being written the descriptor is read first and the data is copied after it: data
is appended, so the copy contains everything that this descriptor describes.

Blocks are preallocated to their maximum size and stay that way until the next
block of the entry starts. Copies skip the unwritten part and keep the file
length, and the local usage counts allocated blocks rather than file lengths.

A change is found either by the `upload` notification or by the scan, which
compares size, modification time and inode with what was copied. The scan is what
finds the data that was there before the archive was enabled, and block rewrites
that replace a file without a sync.

The backend reports `stores_in_place`: the local files are the stored ones, so the
publication of an entry after a burst of writes does not sync each of its files and
leaves that to the periodic sync of the file cache. `upload` can therefore name a
file that is not on the local disk yet, which a copy does not need.

### Removals

Removals on the archive wait until the directory has nothing left to copy, for at
most four times `sync_delay` (at least a minute). Compression removes `.blk` as
soon as `.blk.zst` exists locally, and the archive must not be without both in
between. A removal is dropped when the file exists again. The archive is never
swept for files that are missing locally: only what the engine removed is removed.

### Eviction

When the folder is over its limit, the coldest blocks are chosen, where a block is
as warm as the later of its last modification and its last access. A block is
eligible when its current version is in the archive and it has been idle for
`min_idle`. The newest block of an entry is never evicted: the next record of the
entry goes there, and a write must not depend on the archive being reachable.

1. The worker checks the archive copy (a full read with CRC64 comparison unless
   `RS_ARCHIVE_VERIFY=false`) and writes the stub next to the block under a
   temporary name.
2. The block is handed to the file cache, which drops its descriptor unless the
   block is being accessed or belongs to an open mutation batch.
3. `remove_from_local_cache_if_unused` renames the stub into place, syncs the
   directory and removes the block, unless the block was opened, accessed or
   changed since step 1.

Step 3 runs under the lock of the directory, the same lock `prepare_open` and
`update_local_cache` take, so an eviction and an access of one block are ordered:
the access either stops the eviction or finds the stub and fetches the block back.
Because of that check the backend reports `guards_eviction`, and the file cache
holds a candidate back only for a batch that owns the block itself, not for every
batch open on its entry. An entry that is written all the time always has a batch
open, and its old blocks would otherwise never leave.

### Recall

Opening an evicted block copies it from the archive to a temporary file, checks
the length and the CRC64 recorded in the stub, and renames it into place. The
block then counts as just used and is evicted again only when it is the coldest.

### Archive I/O

Every archive operation is a blocking closure on the blocking pool, at most eight
at a time, each with a time limit. A share that stalls costs eight parked threads
and turns into errors; it cannot hold a lock or a task of the engine.
