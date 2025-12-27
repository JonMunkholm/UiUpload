package handler

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UploadTimeout is the maximum duration for a CSV upload operation.
// Can be overridden for testing or specific use cases.
var UploadTimeout = 5 * time.Minute

// Uploader defines the interface for CSV upload handlers.
// All upload handlers (NsUpload, SfdcUpload, AnrokUpload) implement this interface.
type Uploader interface {
	SetProps() error
}

// BaseUploader provides shared functionality for all CSV upload handlers.
// Embed this struct in specific uploaders (NsUpload, SfdcUpload, AnrokUpload)
// to eliminate code duplication.
type BaseUploader struct {
	Pool   *pgxpool.Pool // Connection pool for transactions and queries
	Root   string
	TopDir string
	DirMap map[string]CsvProps
}

// SetProps initializes the uploader with its root path and directory map.
// Call this from the specific uploader's SetProps method.
func (b *BaseUploader) SetProps(topDir string, makeDirMap func() map[string]CsvProps) error {
	root, err := getUploadsRoot()
	if err != nil {
		return err
	}

	b.TopDir = topDir
	b.Root = filepath.Join(root, topDir)
	b.DirMap = makeDirMap()

	return nil
}

// UploadCsvCheck returns a function that checks if a CSV file has already been uploaded.
func (b *BaseUploader) UploadCsvCheck() CsvCheck {
	return func(ctx context.Context, file string) ([]db.CsvUpload, error) {
		return db.New(b.Pool).GetCsvUpload(ctx, file)
	}
}

// SetActType returns a function that logs an upload action to the csv_uploads table.
func (b *BaseUploader) SetActType(action string) ActType {
	return func(ctx context.Context, fileName string) error {
		params := db.InsertCsvUploadParams{
			Action: action,
			Name:   fileName,
		}

		if err := db.New(b.Pool).InsertCsvUpload(ctx, params); err != nil {
			return fmt.Errorf("failed to record file in csv_uploads DB: %w", err)
		}
		return nil
	}
}

// RunUpload executes the CSV upload process for a specific directory.
func (b *BaseUploader) RunUpload(dir string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), UploadTimeout)
		defer cancel()

		err := ProcessUpload(
			ctx,
			b.Pool,
			b.Root,
			dir,
			b.DirMap,
			b.UploadCsvCheck(),
			b.SetActType("upload"),
		)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return ErrMsg{Err: fmt.Errorf("upload timed out after %v", UploadTimeout)}
			}
			return ErrMsg{Err: err}
		}
		return DoneMsg("Upload Complete!")
	}
}
