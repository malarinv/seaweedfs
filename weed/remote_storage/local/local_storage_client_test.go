package local

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/require"
)

func TestLocalRemoteStorageClientImplementsInterface(t *testing.T) {
	// Compile-time guard: a future breaking change in any of these optional
	// interfaces would be caught at build, not at runtime.
	var (
		_ remote_storage.RemoteStorageClient           = (*localRemoteStorageClient)(nil)
		_ remote_storage.RemoteStorageConcurrentReader = (*localRemoteStorageClient)(nil)
		_ remote_storage.RemoteStorageStreamReader     = (*localRemoteStorageClient)(nil)
	)
}

func TestMakerHasNoBucket(t *testing.T) {
	maker, ok := remote_storage.RemoteStorageClientMakers["local"]
	require.True(t, ok, "local backend must be registered in init()")
	require.False(t, maker.HasBucket(), "local backend does not implement bucket semantics")
}

func TestMakerRequiresLocalPath(t *testing.T) {
	_, err := (&localRemoteStorageMaker{}).Make(&remote_pb.RemoteConf{Name: "test", Type: "local"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "-local.path is required")
}

func TestMakerRejectsMissingRoot(t *testing.T) {
	_, err := (&localRemoteStorageMaker{}).Make(&remote_pb.RemoteConf{
		Name:      "test",
		Type:      "local",
		LocalPath: filepath.Join(t.TempDir(), "does-not-exist"),
	})
	require.Error(t, err)
}

func TestMakerRejectsFileAsRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(root, []byte("x"), 0o600))
	_, err := (&localRemoteStorageMaker{}).Make(&remote_pb.RemoteConf{
		Name:      "test",
		Type:      "local",
		LocalPath: root,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not a directory")
}

func newTestClient(t *testing.T) *localRemoteStorageClient {
	t.Helper()
	root := t.TempDir()
	// layout:
	//   <root>/dir1/fileA   (12 bytes)
	//   <root>/dir1/fileB   (5 bytes)
	//   <root>/dir2/        (empty)
	//   <root>/top.txt      (3 bytes)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dir1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dir1", "fileA"), []byte("hello, world"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dir1", "fileB"), []byte("hello"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dir2"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "top.txt"), []byte("top"), 0o644))

	cli, err := (&localRemoteStorageMaker{}).Make(&remote_pb.RemoteConf{
		Name:      "test_local",
		Type:      "local",
		LocalPath: root,
	})
	require.NoError(t, err)
	return cli.(*localRemoteStorageClient)
}

func TestReadFileFullAndRange(t *testing.T) {
	cli := newTestClient(t)

	data, err := cli.ReadFile(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"}, 0, -1)
	require.NoError(t, err)
	require.Equal(t, "hello, world", string(data))

	// mid-range read
	data, err = cli.ReadFile(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"}, 7, 5)
	require.NoError(t, err)
	require.Equal(t, "world", string(data))
}

func TestReadFileNotFound(t *testing.T) {
	cli := newTestClient(t)
	_, err := cli.ReadFile(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/missing"}, 0, -1)
	require.ErrorIs(t, err, remote_storage.ErrRemoteObjectNotFound)
}

func TestStatFile(t *testing.T) {
	cli := newTestClient(t)
	entry, err := cli.StatFile(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"})
	require.NoError(t, err)
	require.Equal(t, "test_local", entry.StorageName)
	require.Equal(t, int64(12), entry.RemoteSize)
	require.Greater(t, entry.RemoteMtime, int64(0))

	// directory entries report size 0
	entry, err = cli.StatFile(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1"})
	require.NoError(t, err)
	require.Equal(t, int64(0), entry.RemoteSize)

	_, err = cli.StatFile(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/missing"})
	require.ErrorIs(t, err, remote_storage.ErrRemoteObjectNotFound)
}

func TestListDirectory(t *testing.T) {
	cli := newTestClient(t)
	seen := map[string]bool{"/dir1": true, "/dir2": true, "/top.txt": true}
	err := cli.ListDirectory(context.Background(), &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/"}, func(parent string, name string, isDir bool, entry *filer_pb.RemoteEntry) error {
		// the visitor receives the parent directory of the entry plus the
		// entry name. For a top-level entry the parent is "/", so the
		// canonical key is parent+name without a separator.
		key := strings.TrimSuffix(parent, "/") + "/" + name
		switch key {
		case "/dir1", "/dir2":
			require.True(t, isDir)
			require.Nil(t, entry)
		case "/top.txt":
			require.False(t, isDir)
			require.NotNil(t, entry)
			require.Equal(t, int64(3), entry.RemoteSize)
		default:
			t.Fatalf("unexpected entry %q", key)
		}
		delete(seen, key)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, seen, "expected to see all entries exactly once")
}

func TestListDirectoryEmpty(t *testing.T) {
	cli := newTestClient(t)
	calls := 0
	err := cli.ListDirectory(context.Background(), &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir2"}, func(string, string, bool, *filer_pb.RemoteEntry) error {
		calls++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, calls)
}

func TestListDirectoryMissing(t *testing.T) {
	cli := newTestClient(t)
	// a missing path is a no-op, not an error: callers use ListDirectory as a
	// "what's here" probe and an empty result is the right answer for "no
	// such directory" in remote-storage semantics.
	err := cli.ListDirectory(context.Background(), &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/missing"}, func(string, string, bool, *filer_pb.RemoteEntry) error {
		t.Fatal("visitor must not be called for missing directory")
		return nil
	})
	require.NoError(t, err)
}

func TestTraverse(t *testing.T) {
	cli := newTestClient(t)
	got := map[string]int64{}
	err := cli.Traverse(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/"}, func(parent string, name string, isDir bool, entry *filer_pb.RemoteEntry) error {
		if isDir {
			return nil
		}
		key := strings.TrimSuffix(parent, "/") + "/" + name
		got[key] = entry.RemoteSize
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[string]int64{
		"/dir1/fileA": 12,
		"/dir1/fileB": 5,
		"/top.txt":    3,
	}, got)
}

func TestResolveRejectsPathEscape(t *testing.T) {
	cli := newTestClient(t)
	_, err := cli.resolve(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/../escape"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "escapes root")
}

func TestResolveRejectsNUL(t *testing.T) {
	cli := newTestClient(t)
	_ = cli
	_, err := pathFromLocation("/foo\x00bar")
	require.Error(t, err)
}

func TestReadFileAsStreamFull(t *testing.T) {
	cli := newTestClient(t)
	r, err := cli.ReadFileAsStream(context.Background(), &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"}, 0, -1)
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "hello, world", string(data))
}

func TestReadFileAsStreamBoundedSize(t *testing.T) {
	cli := newTestClient(t)
	r, err := cli.ReadFileAsStream(context.Background(), &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"}, 7, 5)
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "world", string(data))
}

func TestReadFileAsStreamOffset(t *testing.T) {
	cli := newTestClient(t)
	r, err := cli.ReadFileAsStream(context.Background(), &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"}, 7, -1)
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "world", string(data))
}

func TestReadFileAsStreamMissing(t *testing.T) {
	cli := newTestClient(t)
	_, err := cli.ReadFileAsStream(context.Background(), &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/missing"}, 0, -1)
	require.ErrorIs(t, err, remote_storage.ErrRemoteObjectNotFound)
}

func TestReadFileWithConcurrencyDelegatesToReadFile(t *testing.T) {
	cli := newTestClient(t)
	data, err := cli.ReadFileWithConcurrency(&remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"}, 0, -1, 8)
	require.NoError(t, err)
	require.Equal(t, "hello, world", string(data))
}

func TestWritesAreUnsupported(t *testing.T) {
	cli := newTestClient(t)
	loc := &remote_pb.RemoteStorageLocation{Name: "test_local", Path: "/dir1/fileA"}
	entry := &filer_pb.Entry{}

	require.ErrorIs(t, cli.WriteDirectory(loc, entry), errReadOnlySentinel)
	require.ErrorIs(t, cli.RemoveDirectory(loc), errReadOnlySentinel)
	_, err := cli.WriteFile(loc, entry, strings.NewReader("data"))
	require.ErrorIs(t, err, errReadOnlySentinel)
	require.ErrorIs(t, cli.UpdateFileMetadata(loc, entry, entry), errReadOnlySentinel)
	require.ErrorIs(t, cli.DeleteFile(loc), errReadOnlySentinel)

	_, err = cli.ListBuckets()
	require.ErrorIs(t, err, errReadOnlySentinel)
	require.ErrorIs(t, cli.CreateBucket("anything"), errReadOnlySentinel)
	require.ErrorIs(t, cli.DeleteBucket("anything"), errReadOnlySentinel)
}

func TestParseRemoteLocationLocalUsesNoBucketParser(t *testing.T) {
	// the local backend registers as non-bucket, so the location parser
	// should produce a RemoteStorageLocation without a Bucket. Anything else
	// would mean a remote of the form "local_hdd/foo/bar" silently mangles
	// the path.
	loc, err := remote_storage.ParseRemoteLocation("local", "test_local/dir1/fileA")
	require.NoError(t, err)
	require.Equal(t, "test_local", loc.Name)
	require.Empty(t, loc.Bucket)
	require.Equal(t, "/dir1/fileA", loc.Path)
}

func TestErrReadOnlySentinelChain(t *testing.T) {
	// errors.Is must walk the chain, so callers can use it to decide whether
	// a write failure is a "no such operation" reply or a real I/O error.
	_, err := (&localRemoteStorageClient{}).WriteFile(&remote_pb.RemoteStorageLocation{}, &filer_pb.Entry{}, strings.NewReader(""))
	require.Error(t, err)
	require.True(t, errors.Is(err, errReadOnlySentinel))
}
