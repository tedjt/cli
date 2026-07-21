package aircmd

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/mutator/aicode"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/dyn"
	"github.com/databricks/cli/libs/vfs"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/jobs"
)

// snapshotViaAicode packages and uploads the code snapshot using the DABs aicode
// mutator, decoupling snapshotting from submission: ephemeral `air run` gets the
// same content-addressed tarball + upload that a DABs deploy would produce.
//
// git resolution stays here (it is a CLI-YAML construct with no DABs config
// surface): materializeSnapshot produces the exact directory to package — a
// git-archived commit, or the working tree — and aicode packages + uploads that
// directory. The rewritten code_source_path is read back off the bundle.
func snapshotViaAicode(ctx context.Context, w *databricks.WorkspaceClient, snap *snapshotSourceConfig, configPath string) (snapshotResult, error) {
	// aicode uploads to the user's workspace-files repo_snapshots dir; a UC Volume
	// target is not wired on this path yet.
	if snap.RemoteVolume != nil {
		return snapshotResult{}, errors.New("code_source.snapshot.remote_volume is not yet supported on the DABs-snapshot path")
	}

	repoPath, err := resolveRootPath(ctx, snap.RootPath, filepath.Dir(configPath))
	if err != nil {
		return snapshotResult{}, err
	}

	codeDir, cleanup, err := materializeSnapshot(ctx, snap, repoPath)
	if err != nil {
		return snapshotResult{}, err
	}
	defer cleanup()

	remote, err := packageDirViaAicode(ctx, w, codeDir)
	if err != nil {
		return snapshotResult{}, err
	}
	return snapshotResult{CodeSourcePath: remote}, nil
}

// materializeSnapshot returns the local directory to package for snap, plus a
// cleanup func for any temp dir it created. For a git ref it archives the resolved
// commit into a temp dir; for the working tree it returns repoPath directly (aicode
// applies .gitignore), or a temp copy of just include_paths when those are set.
func materializeSnapshot(ctx context.Context, snap *snapshotSourceConfig, repoPath string) (string, func(), error) {
	noop := func() {}
	dirName := filepath.Base(repoPath)

	git := newGitRepo(repoPath)
	plan, err := resolveSnapshotPlan(ctx, git, snap.Git, snap.IncludePaths)
	if err != nil {
		return "", noop, err
	}

	if plan.mode == modeGitArchive {
		return materializeGitArchive(ctx, git, plan, dirName)
	}

	// Working tree with no include_paths: package the directory in place.
	if len(snap.IncludePaths) == 0 {
		return repoPath, noop, nil
	}

	// Working tree with include_paths: copy only those subpaths into a temp dir.
	tmp, err := os.MkdirTemp("", "air-snapshot-*")
	if err != nil {
		return "", noop, err
	}
	dest := filepath.Join(tmp, dirName)
	for _, rel := range snap.IncludePaths {
		if err := copyTree(filepath.Join(repoPath, rel), filepath.Join(dest, rel)); err != nil {
			_ = os.RemoveAll(tmp)
			return "", noop, err
		}
	}
	return dest, func() { _ = os.RemoveAll(tmp) }, nil
}

// materializeGitArchive extracts plan.commitSHA (optionally scoped to include_paths)
// into a temp dir via `git archive`, returning the extracted <dirName> directory.
func materializeGitArchive(ctx context.Context, git gitRepo, plan snapshotPlan, dirName string) (string, func(), error) {
	noop := func() {}
	tmp, err := os.MkdirTemp("", "air-snapshot-*")
	if err != nil {
		return "", noop, err
	}
	cleanup := func() { _ = os.RemoveAll(tmp) }

	tarball := filepath.Join(tmp, "archive.tar.gz")
	if err := createGitArchiveSnapshot(ctx, git, plan.commitSHA, tarball, dirName, plan.includePaths); err != nil {
		cleanup()
		return "", noop, err
	}
	if err := extractTarGz(tarball, tmp); err != nil {
		cleanup()
		return "", noop, err
	}
	// createGitArchiveSnapshot prefixes every entry with dirName/, so the tree lands
	// under tmp/<dirName>.
	return filepath.Join(tmp, dirName), cleanup, nil
}

// packageDirViaAicode runs the aicode mutator over a minimal in-memory bundle rooted
// at codeDir's parent, then reads back the code_source_path it rewrote to the
// uploaded archive.
func packageDirViaAicode(ctx context.Context, w *databricks.WorkspaceClient, codeDir string) (string, error) {
	syncRoot := filepath.Dir(codeDir)
	dirName := filepath.Base(codeDir)

	me, err := w.CurrentUser.Me(ctx, iam.MeRequest{})
	if err != nil {
		return "", fmt.Errorf("failed to resolve current user: %w", err)
	}

	b := &bundle.Bundle{
		BundleRootPath: syncRoot,
		BundleRoot:     vfs.MustNew(syncRoot),
		SyncRootPath:   syncRoot,
		SyncRoot:       vfs.MustNew(syncRoot),
		WorktreeRoot:   vfs.MustNew(syncRoot),
		Config: config.Root{
			Bundle: config.Bundle{Target: "default"},
			Workspace: config.Workspace{
				CurrentUser: &config.User{User: me},
			},
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"air": {
						JobSettings: jobs.JobSettings{
							Tasks: []jobs.Task{{
								TaskKey:       "air",
								AiRuntimeTask: &jobs.AiRuntimeTask{CodeSourcePath: dirName},
							}},
						},
					},
				},
			},
		},
	}
	b.SetWorkpaceClient(w)

	// Initialize the dynamic config value from the typed struct so the mutator can
	// read code_source_path via dyn.
	if err := b.Config.Mutate(func(v dyn.Value) (dyn.Value, error) { return v, nil }); err != nil {
		return "", err
	}

	diags := bundle.Apply(ctx, b, aicode.PackageAndUpload())
	if err := diags.Error(); err != nil {
		return "", err
	}

	rewritten, err := dyn.GetByPath(b.Config.Value(),
		dyn.MustPathFromString("resources.jobs.air.tasks[0].ai_runtime_task.code_source_path"))
	if err != nil {
		return "", fmt.Errorf("code snapshot was not packaged: %w", err)
	}
	remote, ok := rewritten.AsString()
	if !ok {
		return "", fmt.Errorf("unexpected code_source_path value %v", rewritten.AsAny())
	}
	return remote, nil
}

// extractTarGz extracts a gzipped tar into dest. Only regular files and directories
// are handled; entries escaping dest (via .. or an absolute name) are rejected.
func extractTarGz(tarball, dest string) error {
	f, err := os.Open(tarball)
	if err != nil {
		return err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(dest, filepath.FromSlash(hdr.Name))
		if !strings.HasPrefix(target, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("archive entry %q escapes the destination", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // git archive output, not untrusted
				out.Close()
				return err
			}
			out.Close()
		}
	}
	return nil
}

// copyTree recursively copies the file or directory at src to dst, skipping symlinks.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return os.MkdirAll(target, 0o755)
		case info.Mode()&os.ModeSymlink != 0:
			return nil
		default:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			return copyFile(p, target, info.Mode())
		}
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
