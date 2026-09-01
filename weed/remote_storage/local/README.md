# local backend

A read-only `RemoteStorageClient` for cache-on-read migrations: serve a host
filesystem tree as a remote storage target, so reads of a SeaweedFS FUSE
mount transparently pull from the legacy path and then live in the chunk
cache.

## Use case

The arr-system migration replaced a `/media/Data` hostPath with a SeaweedFS-
backed RWX PVC. During the transition the legacy HDD still holds the bulk
of the library; copying it into SeaweedFS first would double the disk
footprint. The `local` backend lets the FUSE mount read from the legacy
directory directly:

```
weed shell -h host:port
> remote.configure -name=legacy_hdd -type=local -local.path=/media/Data
> remote.mount -dir=/media -remote=legacy_hdd -nonempty
```

Subsequent reads of `/media/foo.mkv` resolve to `/media/Data/foo.mkv` on
the filer host. The FUSE mount's chunk cache holds each chunk after the
first read, so the second access does not touch the HDD.

## Limitations

- **Read-only.** Writes return a `errReadOnlySentinel` that callers can
  detect with `errors.Is`. This is intentional: the local backend is the
  source of truth during migration, and writing back through it would
  defeat the purpose and risk corrupting the legacy data.
- **No bucket namespace.** One `LocalPath` per remote name, and the
  `remote=local_hdd/foo/bar` syntax resolves `foo/bar` beneath the
  configured root. There is no `-local.bucket` flag.
- **Path-escape guard.** A remote path containing `..` is rejected at
  resolve time, so a misconfigured `local_hdd/../../etc/passwd` cannot
  escape the configured root.

## Configuration

```
remote.configure -name=<name> -type=local -local.path=<absolute path>
```

`-local.path` must point to an existing directory. Misconfiguration
(missing root, file instead of directory) is reported as a config error
and the remote is not registered.

## Tests

```
go test ./weed/remote_storage/local/...
```

The test suite covers maker validation, read paths (full, range,
not-found), directory listing (top-level, empty, missing), traversal,
stream reads, the read-only sentinel chain, and the path-escape guard.
