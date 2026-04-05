//go:build telegram

package telegramingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

type mediaCacheState struct {
	Entries map[string]mediaCacheEntry `json:"entries"`
}

type mediaCacheEntry struct {
	FilePath  string    `json:"file_path"`
	UpdatedAt time.Time `json:"updated_at"`
}

type mediaDownloader struct {
	mu sync.Mutex

	dir       string
	cachePath string
	log       *zap.Logger

	dl *downloader.Downloader

	cache mediaCacheState
}

func newMediaDownloader(opts Options, log *zap.Logger) (*mediaDownloader, error) {
	if !opts.FetchMedia || strings.TrimSpace(opts.MediaDir) == "" {
		return nil, nil
	}
	cachePath := strings.TrimSpace(opts.MediaCachePath)
	if cachePath == "" {
		cachePath = filepath.Join(opts.MediaDir, "cache.json")
	}
	if err := os.MkdirAll(opts.MediaDir, 0o755); err != nil {
		return nil, err
	}

	m := &mediaDownloader{
		dir:       opts.MediaDir,
		cachePath: cachePath,
		log:       log,
		dl:        downloader.NewDownloader().WithAllowCDN(true),
		cache: mediaCacheState{
			Entries: make(map[string]mediaCacheEntry, 2048),
		},
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *mediaDownloader) Close() error {
	if m == nil {
		return nil
	}
	return m.save()
}

func (m *mediaDownloader) Resolve(ctx context.Context, api *tg.Client, rec *domain.MessageRecord, src *tg.Message) (string, error) {
	if m == nil || api == nil || rec == nil || src == nil || src.Media == nil {
		return "", nil
	}

	var (
		loc      tg.InputFileLocationClass
		mimeType string
	)

	switch media := src.Media.(type) {
	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.AsNotEmpty()
		if !ok {
			return "", nil
		}
		if rec.MediaID == "" {
			rec.MediaID = strconv.FormatInt(photo.GetID(), 10)
		}
		loc = &tg.InputPhotoFileLocation{
			ID:            photo.GetID(),
			AccessHash:    photo.GetAccessHash(),
			FileReference: photo.GetFileReference(),
			ThumbSize:     pickPhotoThumbType(photo.GetSizes()),
		}
		mimeType = "image/jpeg"
	case *tg.MessageMediaDocument:
		doc, ok := media.Document.AsNotEmpty()
		if !ok {
			return "", nil
		}
		if rec.MediaID == "" {
			rec.MediaID = strconv.FormatInt(doc.GetID(), 10)
		}
		if rec.MediaFileName == "" {
			rec.MediaFileName = documentFileName(doc)
		}
		mimeType = normalizeMIME(doc.GetMimeType())
		loc = &tg.InputDocumentFileLocation{
			ID:            doc.GetID(),
			AccessHash:    doc.GetAccessHash(),
			FileReference: doc.GetFileReference(),
			ThumbSize:     "",
		}
	default:
		return "", nil
	}

	cacheKey := mediaCacheKey(*rec)
	if cacheKey == "" {
		return "", nil
	}

	m.mu.Lock()
	if cached, ok := m.cache.Entries[cacheKey]; ok {
		if cached.FilePath != "" && mediaFileExists(cached.FilePath) {
			m.mu.Unlock()
			return cached.FilePath, nil
		}
		delete(m.cache.Entries, cacheKey)
	}
	m.mu.Unlock()

	target := m.mediaPath(*rec, mimeType)
	if mediaFileExists(target) {
		m.mu.Lock()
		m.cache.Entries[cacheKey] = mediaCacheEntry{FilePath: target, UpdatedAt: time.Now().UTC()}
		m.mu.Unlock()
		return target, nil
	}

	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", err
	}
	part := target + ".part"
	_ = os.Remove(part)
	if _, err := m.dl.Download(api, loc).ToPath(ctx, part); err != nil {
		_ = os.Remove(part)
		return "", err
	}
	if err := os.Rename(part, target); err != nil {
		_ = os.Remove(part)
		return "", err
	}

	m.mu.Lock()
	m.cache.Entries[cacheKey] = mediaCacheEntry{FilePath: target, UpdatedAt: time.Now().UTC()}
	m.mu.Unlock()
	return target, nil
}

func (m *mediaDownloader) mediaPath(rec domain.MessageRecord, mimeType string) string {
	account := sanitizePathPart(rec.AccountID)
	chat := strconv.FormatInt(rec.ChatID, 10)
	kind := sanitizePathPart(rec.MediaType)
	baseID := firstNonEmpty(strings.TrimSpace(rec.MediaID), strings.TrimSpace(rec.MediaFileHash), fmt.Sprintf("msg_%d", rec.MessageID))
	baseID = sanitizePathPart(baseID)
	name := baseID
	if rawName := strings.TrimSpace(rec.MediaFileName); rawName != "" {
		ext := strings.ToLower(filepath.Ext(rawName))
		plain := strings.TrimSuffix(filepath.Base(rawName), ext)
		plain = sanitizePathPart(plain)
		if plain != "" {
			name = plain + "_" + baseID
		}
	}
	ext := inferMediaExtension(rec.MediaType, rec.MediaFileName, mimeType)
	return filepath.Join(m.dir, account, chat, kind, name+ext)
}

func (m *mediaDownloader) load() error {
	b, err := os.ReadFile(m.cachePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var st mediaCacheState
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Entries == nil {
		st.Entries = make(map[string]mediaCacheEntry)
	}
	m.cache = st
	return nil
}

func (m *mediaDownloader) save() error {
	m.mu.Lock()
	b, err := json.MarshalIndent(m.cache, "", "  ")
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.cachePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(m.cachePath, b, 0o600)
}

func mediaCacheKey(rec domain.MessageRecord) string {
	id := firstNonEmpty(strings.TrimSpace(rec.MediaID), strings.TrimSpace(rec.MediaFileHash))
	if id == "" {
		return ""
	}
	return strings.Join([]string{
		sanitizePathPart(rec.AccountID),
		strconv.FormatInt(rec.ChatID, 10),
		sanitizePathPart(rec.MediaType),
		sanitizePathPart(id),
	}, ":")
}

func sanitizePathPart(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
		" ", "_",
	)
	v = replacer.Replace(v)
	if len(v) > 100 {
		v = v[:100]
	}
	if v == "" {
		return "unknown"
	}
	return v
}

func pickPhotoThumbType(sizes []tg.PhotoSizeClass) string {
	best := ""
	bestScore := -1
	for _, sz := range sizes {
		if sz == nil {
			continue
		}
		t := strings.TrimSpace(sz.GetType())
		if t == "" {
			continue
		}
		score := photoThumbScore(t)
		if score > bestScore {
			best = t
			bestScore = score
		}
	}
	if best == "" {
		return "x"
	}
	return best
}

func photoThumbScore(t string) int {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "w":
		return 100
	case "y":
		return 95
	case "x":
		return 90
	case "m":
		return 80
	case "q":
		return 75
	case "i":
		return 70
	case "s":
		return 60
	default:
		return 50
	}
}

func inferMediaExtension(mediaType, fileName, mimeType string) string {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(fileName)))
	if ext != "" && len(ext) <= 10 {
		return ext
	}
	mimeType = normalizeMIME(mimeType)
	if mimeType != "" {
		if exts, err := mime.ExtensionsByType(mimeType); err == nil && len(exts) > 0 {
			return exts[0]
		}
	}
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "photo":
		return ".jpg"
	case "video", "video_note":
		return ".mp4"
	case "audio":
		return ".mp3"
	case "voice":
		return ".ogg"
	case "sticker":
		return ".webp"
	case "gif":
		return ".gif"
	default:
		return ".bin"
	}
}

func documentFileName(doc *tg.Document) string {
	if doc == nil {
		return ""
	}
	for _, attr := range doc.Attributes {
		if f, ok := attr.(*tg.DocumentAttributeFilename); ok {
			return strings.TrimSpace(f.FileName)
		}
	}
	return ""
}

func normalizeMIME(v string) string {
	v = strings.TrimSpace(strings.ToLower(v))
	if idx := strings.IndexByte(v, ';'); idx >= 0 {
		v = strings.TrimSpace(v[:idx])
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func mediaFileExists(path string) bool {
	if path == "" {
		return false
	}
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !st.IsDir()
}
