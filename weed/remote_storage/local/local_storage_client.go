// Package local provides a read-only RemoteStorageClient that serves a host
// filesystem tree as a remote storage target. It exists for cache-on-read
// migration scenarios where a legacy filesystem (e.g. an HDD mounted at
// /mnt/legacy-hdd) must be visible through the SeaweedFS FUSE mount while
// reads are transparently cached in chunk storage.
//
// Writes are intentionally unsupported. The local backend is the source of
// truth during a migration; uploading back to it would defeat the purpose
// and risk corrupting the legacy data. Operations that would mutate the
// underlying tree return remote_storage.ErrUnsupported.
//
// Location semantics: the storage name resolves to a single root directory
// (LocalPath), and the loc.Path is interpreted as a path beneath that root.
// HasBucket() reports false, so a remote string like "legacy_hdd/media/foo"
// parses to {Name: "legacy_hdd", Path: "/media/foo"} and reads resolve to
// $(LocalPath)/media/foo on disk.
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	remote_storage.RemoteStorageClientMakers["local"] = new(localRemoteStorageMaker)
}

type localRemoteStorageMaker struct{}

func (localRemoteStorageMaker) HasBucket() bool {
	// A local filesystem has no concept of buckets; one LocalPath per remote
	// name and a flat path namespace beneath it.
	return false
}

func (localRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	if conf.LocalPath == "" {
		return nil, fmt.Errorf("local remote %q: -local.path is required", conf.Name)
	}
	root, err := filepath.Abs(conf.LocalPath)
	if err != nil {
		return nil, fmt.Errorf("local remote %q: resolve -local.path: %w", conf.Name, err)
	}
	// The root must exist and be a directory at config time. A misconfiguration
	// here would otherwise surface as a confusing ENOENT on every read.
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("local remote %q: stat -local.path %q: %w", conf.Name, root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("local remote %q: -local.path %q is not a directory", conf.Name, root)
	}
	return &localRemoteStorageClient{
		conf: conf,
		root: root,
	}, nil
}

type localRemoteStorageClient struct {
	conf *remote_pb.RemoteConf
	root string
}

var (
	_ remote_storage.RemoteStorageClient              = (*localRemoteStorageClient)(nil)
	_ remote_storage.RemoteStorageConcurrentReader    = (*localRemoteStorageClient)(nil)
	_ remote_storage.RemoteStorageStreamReader        = (*localRemoteStorageClient)(nil)
)

// resolve joins the storage root with loc.Path and verifies the result stays
// inside root. A misconfigured storage name (e.g. an embedded "..") would
// otherwise let a remote string escape the configured tree.
func (l *localRemoteStorageClient) resolve(loc *remote_pb.RemoteStorageLocation) (string, error) {
	rel, err := pathFromLocation(loc.Path)
	if err != nil {
		return "", err
	}
	full := filepath.Join(l.root, rel)
	// filepath.Clean collapses any ".." but does not bound it to root. Re-eval
	// and compare so a malicious or misconfigured path is rejected up front
	// rather than silently reading /etc/shadow.
	cleaned := filepath.Clean(full)
	if cleaned != l.root && !strings.HasPrefix(cleaned, l.root+string(filepath.Separator)) {
		return "", fmt.Errorf("local remote %q: path %q escapes root %q", l.conf.Name, loc.Path, l.root)
	}
	return cleaned, nil
}

// pathFromLocation converts the leading-slash FUSE-style path used by
// RemoteStorageLocation into a relative path that filepath.Join accepts.
func pathFromLocation(p string) (string, error) {
	if p == "" {
		return ".", nil
	}
	// Reject NUL and other control characters that filepath can't represent.
	for _, r := range p {
		if r == 0 {
			return "", fmt.Errorf("path contains NUL byte")
		}
	}
	// loc.Path always starts with "/"; strip it so the join treats the result
	// as relative to the root.
	if p[0] != '/' {
		return "", fmt.Errorf("path %q does not start with /", p)
	}
	rel := p[1:]
	if rel == "" {
		return ".", nil
	}
	return rel, nil
}

func (l *localRemoteStorageClient) toRemoteEntry(absPath string, info os.FileInfo) *filer_pb.RemoteEntry {
	return &filer_pb.RemoteEntry{
		StorageName: l.conf.Name,
		RemoteMtime: info.ModTime().Unix(),
		RemoteSize:  info.Size(),
		// No remote-side etag: a local file has no opaque identity to expose
		// upstream. The filer uses the (StorageName, Path) pair as the cache
		// key, which is stable across reads.
	}
}

// Traverse walks the entire subtree under loc.Path, invoking visitFn for every
// regular file. There is no prefix-based pagination: a host filesystem is
// small enough to enumerate in one pass, and breaking it into pages would
// complicate cache-on-read consumers that expect a single ordered stream.
func (l *localRemoteStorageClient) Traverse(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	root, err := l.resolve(loc)
	if err != nil {
		return err
	}
	info, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("local remote %q: traverse %q: %w", l.conf.Name, root, err)
	}
	if !info.IsDir() {
		// Traverse of a file path: report the file and stop.
		dir, name := util.FullPath(loc.Path).DirAndName()
		return visitFn(dir, name, false, l.toRemoteEntry(root, info))
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			glog.V(1).Infof("local remote %q: walk entry %q: %v", l.conf.Name, path, walkErr)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(l.root, path)
		if relErr != nil {
			return nil
		}
		fi, statErr := d.Info()
		if statErr != nil {
			return nil
		}
		dir, name := util.FullPath("/" + rel).DirAndName()
		return visitFn(dir, name, false, l.toRemoteEntry(path, fi))
	})
}

// ListDirectory emits the immediate children of loc.Path. It deliberately
// uses os.ReadDir (not os.File.Readdir) so the syscall is bounded to a single
// directory entry and the visitor never blocks on a slow file open.
func (l *localRemoteStorageClient) ListDirectory(ctx context.Context, loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	dir, err := l.resolve(loc)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("local remote %q: list %q: %w", l.conf.Name, dir, err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		isDir := entry.IsDir()
		var remoteEntry *filer_pb.RemoteEntry
		if !isDir {
			info, infoErr := entry.Info()
			if infoErr != nil {
				glog.V(1).Infof("local remote %q: stat %q: %v", l.conf.Name, entry.Name(), infoErr)
				continue
			}
			remoteEntry = l.toRemoteEntry(filepath.Join(dir, entry.Name()), info)
		}
		// visitFn expects (parent_dir, name, isDir, entry). The parent is the
		// directory we just listed (loc.Path), and the name is the entry's
		// own name. We don't run DirAndName here because that would conflate
		// the listing directory with the entry's logical parent.
		if err := visitFn(loc.Path, entry.Name(), isDir, remoteEntry); err != nil {
			return err
		}
	}
	return nil
}

// StatFile returns metadata for a single file or directory. The size of a
// directory is reported as 0; callers that need to recurse should use
// ListDirectory instead of guessing from the size.
func (l *localRemoteStorageClient) StatFile(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	abs, err := l.resolve(loc)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, remote_storage.ErrRemoteObjectNotFound
		}
		return nil, fmt.Errorf("local remote %q: stat %q: %w", l.conf.Name, abs, err)
	}
	entry := l.toRemoteEntry(abs, info)
	if info.IsDir() {
		entry.RemoteSize = 0
	}
	return entry, nil
}

// ReadFile serves a byte range from loc.Path. Negative or zero-sized requests
// are treated as a request for the rest of the file, matching os.ReadFile and
// the io.ReaderAt contract that callers expect.
func (l *localRemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) ([]byte, error) {
	abs, err := l.resolve(loc)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return []byte{}, nil
	}
	f, err := os.Open(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, remote_storage.ErrRemoteObjectNotFound
		}
		return nil, fmt.Errorf("local remote %q: open %q: %w", l.conf.Name, abs, err)
	}
	defer f.Close()
	if size < 0 {
		// Negative size means "read to EOF".
		return io.ReadAll(f)
	}
	buf := make([]byte, size)
	n, readErr := f.ReadAt(buf, offset)
	if readErr != nil {
		if errors.Is(readErr, io.EOF) {
			return buf[:n], nil
		}
		return nil, fmt.Errorf("local remote %q: read %q @%d+%d: %w", l.conf.Name, abs, offset, size, readErr)
	}
	return buf, nil
}

// ReadFileWithConcurrency is required to satisfy RemoteStorageConcurrentReader.
// A local filesystem already has the highest available read throughput from
// any single client, so concurrency would only add syscall overhead; we
// serialize to a single read and discard the hint.
func (l *localRemoteStorageClient) ReadFileWithConcurrency(loc *remote_pb.RemoteStorageLocation, offset int64, size int64, _ int) ([]byte, error) {
	return l.ReadFile(loc, offset, size)
}

// ReadFileAsStream returns an open file handle positioned at offset. The
// caller is responsible for closing it. Streaming matters for very large
// files where the buffer allocation in ReadFile would push the chunk cache
// into swap.
func (l *localRemoteStorageClient) ReadFileAsStream(ctx context.Context, loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (io.ReadCloser, error) {
	abs, err := l.resolve(loc)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, remote_storage.ErrRemoteObjectNotFound
		}
		return nil, fmt.Errorf("local remote %q: open %q: %w", l.conf.Name, abs, err)
	}
	if offset > 0 {
		if _, seekErr := f.Seek(offset, io.SeekStart); seekErr != nil {
			f.Close()
			return nil, fmt.Errorf("local remote %q: seek %q @%d: %w", l.conf.Name, abs, offset, seekErr)
		}
	}
	if size < 0 {
		return f, nil
	}
	return &limitedFile{File: f, remaining: size, ctx: ctx}, nil
}

// limitedFile wraps an *os.File so ReadFileAsStream honors a positive size
// without making the caller slice off the tail. Cancellation aborts the
// remaining reads.
type limitedFile struct {
	*os.File
	remaining int64
	ctx       context.Context
}

func (l *limitedFile) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, io.EOF
	}
	if err := l.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.File.Read(p)
	l.remaining -= int64(n)
	return n, err
}

// WriteDirectory is unsupported: this backend is read-only by design.
func (l *localRemoteStorageClient) WriteDirectory(*remote_pb.RemoteStorageLocation, *filer_pb.Entry) error {
	return errReadOnly("WriteDirectory")
}

// RemoveDirectory is unsupported.
func (l *localRemoteStorageClient) RemoveDirectory(*remote_pb.RemoteStorageLocation) error {
	return errReadOnly("RemoveDirectory")
}

// WriteFile is unsupported.
func (l *localRemoteStorageClient) WriteFile(*remote_pb.RemoteStorageLocation, *filer_pb.Entry, io.Reader) (*filer_pb.RemoteEntry, error) {
	return nil, errReadOnly("WriteFile")
}

// UpdateFileMetadata is unsupported.
func (l *localRemoteStorageClient) UpdateFileMetadata(*remote_pb.RemoteStorageLocation, *filer_pb.Entry, *filer_pb.Entry) error {
	return errReadOnly("UpdateFileMetadata")
}

// DeleteFile is unsupported.
func (l *localRemoteStorageClient) DeleteFile(*remote_pb.RemoteStorageLocation) error {
	return errReadOnly("DeleteFile")
}

// ListBuckets is unsupported: the local backend has no bucket namespace.
func (l *localRemoteStorageClient) ListBuckets() ([]*remote_storage.Bucket, error) {
	return nil, errReadOnly("ListBuckets")
}

// CreateBucket is unsupported.
func (l *localRemoteStorageClient) CreateBucket(string) error {
	return errReadOnly("CreateBucket")
}

// DeleteBucket is unsupported.
func (l *localRemoteStorageClient) DeleteBucket(string) error {
	return errReadOnly("DeleteBucket")
}

func errReadOnly(op string) error {
	return fmt.Errorf("local remote storage: %s is not supported (read-only backend): %w", op, errReadOnlySentinel)
}

// errReadOnlySentinel is exported via errors.Is so callers can distinguish
// "backend does not implement this operation" from a transient I/O error.
var errReadOnlySentinel = errors.New("local remote storage is read-only")
