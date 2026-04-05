//go:build telegram

package telegramingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
)

type avatarCacheState struct {
	Entries map[string]avatarCacheEntry `json:"entries"`
}

type avatarCacheEntry struct {
	FilePath  string    `json:"file_path"`
	UpdatedAt time.Time `json:"updated_at"`
}

type avatarDownloader struct {
	mu sync.Mutex

	dir       string
	cachePath string
	big       bool
	log       *zap.Logger

	dl *downloader.Downloader

	cache avatarCacheState
}

func newAvatarDownloader(opts Options, log *zap.Logger) (*avatarDownloader, error) {
	if !opts.FetchAvatars || opts.AvatarDir == "" {
		return nil, nil
	}
	cachePath := opts.AvatarCachePath
	if cachePath == "" {
		cachePath = filepath.Join(opts.AvatarDir, "cache.json")
	}
	if err := os.MkdirAll(opts.AvatarDir, 0o755); err != nil {
		return nil, err
	}

	a := &avatarDownloader{
		dir:       opts.AvatarDir,
		cachePath: cachePath,
		big:       opts.AvatarBig,
		log:       log,
		dl:        downloader.NewDownloader().WithAllowCDN(true),
		cache: avatarCacheState{
			Entries: make(map[string]avatarCacheEntry, 1024),
		},
	}
	if err := a.load(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *avatarDownloader) Close() error {
	if a == nil {
		return nil
	}
	return a.save()
}

func (a *avatarDownloader) Resolve(ctx context.Context, api *tg.Client, u *tg.User) (ref string, file string, err error) {
	if a == nil || u == nil || api == nil {
		return "", "", nil
	}
	photoClass, ok := u.GetPhoto()
	if !ok {
		return "", "", nil
	}
	photo, ok := photoClass.AsNotEmpty()
	if !ok {
		return "", "", nil
	}

	userID := u.GetID()
	photoID := photo.GetPhotoID()
	ref = fmt.Sprintf("tg://userphoto/%d/%d", userID, photoID)
	cacheKey := avatarCacheKey(userID, photoID, a.big)

	a.mu.Lock()
	if cached, ok := a.cache.Entries[cacheKey]; ok {
		if cached.FilePath != "" && fileExists(cached.FilePath) {
			a.mu.Unlock()
			return ref, cached.FilePath, nil
		}
		delete(a.cache.Entries, cacheKey)
	}
	a.mu.Unlock()

	quality := "small"
	if a.big {
		quality = "big"
	}
	finalPath := filepath.Join(a.dir, fmt.Sprintf("%d_%d_%s.jpg", userID, photoID, quality))
	if fileExists(finalPath) {
		a.mu.Lock()
		a.cache.Entries[cacheKey] = avatarCacheEntry{FilePath: finalPath, UpdatedAt: time.Now().UTC()}
		a.mu.Unlock()
		return ref, finalPath, nil
	}

	loc := &tg.InputPeerPhotoFileLocation{
		Peer:    u.AsInputPeer(),
		PhotoID: photoID,
	}
	loc.SetBig(a.big)

	tmpPath := finalPath + ".part"
	_ = os.Remove(tmpPath)
	if _, err := a.dl.Download(api, loc).ToPath(ctx, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return ref, "", err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return ref, "", err
	}

	a.mu.Lock()
	a.cache.Entries[cacheKey] = avatarCacheEntry{FilePath: finalPath, UpdatedAt: time.Now().UTC()}
	a.mu.Unlock()

	return ref, finalPath, nil
}

func (a *avatarDownloader) load() error {
	b, err := os.ReadFile(a.cachePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var st avatarCacheState
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Entries == nil {
		st.Entries = make(map[string]avatarCacheEntry)
	}
	a.cache = st
	return nil
}

func (a *avatarDownloader) save() error {
	a.mu.Lock()
	b, err := json.MarshalIndent(a.cache, "", "  ")
	a.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(a.cachePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(a.cachePath, b, 0o600)
}

func avatarCacheKey(userID, photoID int64, big bool) string {
	if big {
		return fmt.Sprintf("%d:%d:1", userID, photoID)
	}
	return fmt.Sprintf("%d:%d:0", userID, photoID)
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !st.IsDir()
}
