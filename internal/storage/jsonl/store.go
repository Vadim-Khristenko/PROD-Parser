package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

const defaultMaxMessagesPerFile = 5000

type chatWriter struct {
	mu                 sync.Mutex
	key                string
	dir                string
	segment            int
	path               string
	file               *os.File
	writer             *bufio.Writer
	offset             int64
	messageCount       int
	maxMessagesPerFile int
}

// Store writes canonical messages into partitioned JSONL files.
type Store struct {
	root string

	mu                 sync.Mutex
	files              map[string]*chatWriter
	maxMessagesPerFile int
}

type ChatReadProgress struct {
	FilePath          string
	FileIndex         int
	FileCount         int
	FileMessagesRead  int
	TotalMessagesRead int
	Pass              int
	Done              bool
}

func NewStore(root string) *Store {
	return &Store{
		root:               root,
		files:              make(map[string]*chatWriter, 128),
		maxMessagesPerFile: defaultMaxMessagesPerFile,
	}
}

func (s *Store) SetMaxMessagesPerFile(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit < 0 {
		limit = 0
	}
	s.maxMessagesPerFile = limit
	for _, cw := range s.files {
		cw.maxMessagesPerFile = limit
	}
}

func (s *Store) AppendBatch(ctx context.Context, msgs []domain.MessageRecord) ([]domain.StoredPointer, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	buckets := map[string][]domain.MessageRecord{}
	for _, m := range msgs {
		buckets[m.ChatKey()] = append(buckets[m.ChatKey()], m)
	}

	result := make([]domain.StoredPointer, 0, len(msgs))
	for _, batch := range buckets {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		ptrs, err := s.appendToChatFile(batch)
		if err != nil {
			return nil, err
		}
		result = append(result, ptrs...)
	}
	return result, nil
}

func (s *Store) ReadByPointer(ptr domain.StoredPointer) (domain.MessageRecord, error) {
	f, err := os.Open(ptr.FilePath)
	if err != nil {
		return domain.MessageRecord{}, err
	}
	defer f.Close()

	if _, err := f.Seek(ptr.Offset, io.SeekStart); err != nil {
		return domain.MessageRecord{}, err
	}
	buf := make([]byte, ptr.Length)
	if _, err := io.ReadFull(f, buf); err != nil {
		return domain.MessageRecord{}, err
	}
	if len(buf) > 0 && buf[len(buf)-1] == '\n' {
		buf = buf[:len(buf)-1]
	}
	var rec domain.MessageRecord
	if err := json.Unmarshal(buf, &rec); err != nil {
		return domain.MessageRecord{}, err
	}
	return rec, nil
}

func (s *Store) ReadChat(accountID string, chatID int64) ([]domain.MessageRecord, error) {
	return s.ReadChatWithProgress(accountID, chatID, 0, nil)
}

func (s *Store) ReadChatUserContext(
	accountID string,
	chatID int64,
	userID int64,
	progressEvery int,
	onProgress func(ChatReadProgress),
) ([]domain.MessageRecord, error) {
	if userID <= 0 {
		return nil, errors.New("user id must be positive")
	}
	paths, err := s.listChatFiles(accountID, chatID)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}

	ownMessageIDs := make(map[int]struct{}, 4096)
	totalRead := 0
	for i, path := range paths {
		readCount, err := collectUserMessageIDs(path, userID, progressEvery, func(fileRead int, done bool) {
			if onProgress == nil {
				return
			}
			onProgress(ChatReadProgress{
				FilePath:          path,
				FileIndex:         i + 1,
				FileCount:         len(paths),
				FileMessagesRead:  fileRead,
				TotalMessagesRead: totalRead + fileRead,
				Pass:              1,
				Done:              done,
			})
		}, ownMessageIDs)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		totalRead += readCount
	}

	out := make([]domain.MessageRecord, 0, 2048)
	totalRead = 0
	for i, path := range paths {
		chunk, readCount, err := readJSONLFileUserContext(path, userID, ownMessageIDs, progressEvery, func(fileRead int, done bool) {
			if onProgress == nil {
				return
			}
			onProgress(ChatReadProgress{
				FilePath:          path,
				FileIndex:         i + 1,
				FileCount:         len(paths),
				FileMessagesRead:  fileRead,
				TotalMessagesRead: totalRead + fileRead,
				Pass:              2,
				Done:              done,
			})
		})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		totalRead += readCount
		out = append(out, chunk...)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Date.Equal(out[j].Date) {
			return out[i].MessageID < out[j].MessageID
		}
		return out[i].Date.Before(out[j].Date)
	})
	return out, nil
}

func (s *Store) ReadChatWithProgress(
	accountID string,
	chatID int64,
	progressEvery int,
	onProgress func(ChatReadProgress),
) ([]domain.MessageRecord, error) {
	paths, err := s.listChatFiles(accountID, chatID)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}

	out := make([]domain.MessageRecord, 0, 1024)
	totalRead := 0
	for i, path := range paths {
		chunk, err := readJSONLFileWithProgress(path, progressEvery, func(fileRead int, done bool) {
			if onProgress == nil {
				return
			}
			onProgress(ChatReadProgress{
				FilePath:          path,
				FileIndex:         i + 1,
				FileCount:         len(paths),
				FileMessagesRead:  fileRead,
				TotalMessagesRead: totalRead + fileRead,
				Done:              done,
			})
		})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		totalRead += len(chunk)
		out = append(out, chunk...)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Date.Equal(out[j].Date) {
			return out[i].MessageID < out[j].MessageID
		}
		return out[i].Date.Before(out[j].Date)
	})
	return out, nil
}

func (s *Store) MaxMessageID(accountID string, chatID int64) (int, error) {
	paths, err := s.listChatFiles(accountID, chatID)
	if err != nil {
		return 0, err
	}
	if len(paths) == 0 {
		return 0, nil
	}

	maxID := 0
	for _, path := range paths {
		v, err := scanMaxMessageID(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, err
		}
		if v > maxID {
			maxID = v
		}
	}
	return maxID, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	files := make([]*chatWriter, 0, len(s.files))
	for _, f := range s.files {
		files = append(files, f)
	}
	s.mu.Unlock()

	var firstErr error
	for _, cw := range files {
		cw.mu.Lock()
		if cw.writer != nil {
			if err := cw.writer.Flush(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if cw.file != nil {
			if err := cw.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		cw.mu.Unlock()
	}
	return firstErr
}

func (s *Store) appendToChatFile(msgs []domain.MessageRecord) ([]domain.StoredPointer, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	key := msgs[0].ChatKey()
	cw, err := s.getWriter(key, msgs[0].AccountID, msgs[0].ChatID)
	if err != nil {
		return nil, err
	}

	cw.mu.Lock()
	defer cw.mu.Unlock()

	ptrs := make([]domain.StoredPointer, 0, len(msgs))
	for _, m := range msgs {
		if cw.maxMessagesPerFile > 0 && cw.messageCount >= cw.maxMessagesPerFile {
			if err := s.rotateWriter(cw); err != nil {
				return nil, err
			}
		}

		line, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		off := cw.offset
		if _, err := cw.writer.Write(line); err != nil {
			return nil, err
		}
		if err := cw.writer.WriteByte('\n'); err != nil {
			return nil, err
		}
		cw.offset += int64(len(line) + 1)
		cw.messageCount++
		ptrs = append(ptrs, domain.StoredPointer{
			InternalID: m.InternalID,
			AccountID:  m.AccountID,
			ChatID:     m.ChatID,
			MessageID:  m.MessageID,
			FilePath:   cw.path,
			Offset:     off,
			Length:     int32(len(line) + 1),
		})
	}
	if err := cw.writer.Flush(); err != nil {
		return nil, err
	}
	return ptrs, nil
}

func (s *Store) rotateWriter(cw *chatWriter) error {
	if cw == nil {
		return errors.New("nil chat writer")
	}
	if cw.writer != nil {
		if err := cw.writer.Flush(); err != nil {
			return err
		}
	}
	if cw.file != nil {
		if err := cw.file.Close(); err != nil {
			return err
		}
	}

	segment, path, messageCount, err := resolveSegmentForAppend(cw.dir, cw.maxMessagesPerFile)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if !st.Mode().IsRegular() {
		_ = f.Close()
		return errors.New("messages path is not regular file")
	}

	cw.segment = segment
	cw.path = path
	cw.file = f
	cw.writer = bufio.NewWriterSize(f, 1<<20)
	cw.offset = st.Size()
	cw.messageCount = messageCount
	return nil
}

func (s *Store) getWriter(key, accountID string, chatID int64) (*chatWriter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cw, ok := s.files[key]; ok {
		return cw, nil
	}
	dir := filepath.Join(s.root, "jsonl", accountID, fmt.Sprintf("%d", chatID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	maxPerFile := s.maxMessagesPerFile
	legacyPath := filepath.Join(dir, "messages.jsonl")
	if fileExists(legacyPath) {
		count, err := countJSONLLines(legacyPath)
		if err != nil {
			return nil, err
		}
		f, err := os.OpenFile(legacyPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}
		st, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if !st.Mode().IsRegular() {
			_ = f.Close()
			return nil, errors.New("messages path is not regular file")
		}
		cw := &chatWriter{
			key:                key,
			dir:                dir,
			segment:            0,
			path:               legacyPath,
			file:               f,
			writer:             bufio.NewWriterSize(f, 1<<20),
			offset:             st.Size(),
			messageCount:       count,
			maxMessagesPerFile: maxPerFile,
		}
		s.files[key] = cw
		return cw, nil
	}

	segment, path, count, err := resolveSegmentForAppend(dir, maxPerFile)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !st.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.New("messages path is not regular file")
	}
	cw := &chatWriter{
		key:                key,
		dir:                dir,
		segment:            segment,
		path:               path,
		file:               f,
		writer:             bufio.NewWriterSize(f, 1<<20),
		offset:             st.Size(),
		messageCount:       count,
		maxMessagesPerFile: maxPerFile,
	}
	s.files[key] = cw
	return cw, nil
}

func (s *Store) listChatFiles(accountID string, chatID int64) ([]string, error) {
	dir := filepath.Join(s.root, "jsonl", accountID, fmt.Sprintf("%d", chatID))
	segmented, err := filepath.Glob(filepath.Join(dir, "messages_*.jsonl"))
	if err != nil {
		return nil, err
	}
	sort.Strings(segmented)

	legacyPath := filepath.Join(dir, "messages.jsonl")
	paths := make([]string, 0, len(segmented)+1)
	if fileExists(legacyPath) {
		paths = append(paths, legacyPath)
	}
	paths = append(paths, segmented...)
	return paths, nil
}

func readJSONLFile(path string) ([]domain.MessageRecord, error) {
	return readJSONLFileWithProgress(path, 0, nil)
}

func collectUserMessageIDs(
	path string,
	userID int64,
	progressEvery int,
	onProgress func(read int, done bool),
	messageIDs map[int]struct{},
) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 * 1024 * 1024
	scanner.Buffer(make([]byte, 1024), maxLine)

	read := 0
	for scanner.Scan() {
		var row struct {
			MessageID  int   `json:"message_id"`
			FromUserID int64 `json:"from_user_id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return 0, err
		}
		if row.FromUserID == userID && row.MessageID > 0 {
			messageIDs[row.MessageID] = struct{}{}
		}
		read++
		if onProgress != nil && progressEvery > 0 && read%progressEvery == 0 {
			onProgress(read, false)
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if onProgress != nil {
		onProgress(read, true)
	}
	return read, nil
}

func readJSONLFileUserContext(
	path string,
	userID int64,
	ownMessageIDs map[int]struct{},
	progressEvery int,
	onProgress func(read int, done bool),
) ([]domain.MessageRecord, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 * 1024 * 1024
	scanner.Buffer(make([]byte, 1024), maxLine)

	out := make([]domain.MessageRecord, 0, 1024)
	read := 0
	for scanner.Scan() {
		var m domain.MessageRecord
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			return nil, 0, err
		}
		include := false
		if m.FromUserID == userID {
			include = true
		} else {
			for _, uid := range m.MentionsUserIDs {
				if uid == userID {
					include = true
					break
				}
			}
			if !include && m.ReplyToMsgID != nil {
				_, include = ownMessageIDs[*m.ReplyToMsgID]
			}
		}
		if include {
			out = append(out, m)
		}
		read++
		if onProgress != nil && progressEvery > 0 && read%progressEvery == 0 {
			onProgress(read, false)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	if onProgress != nil {
		onProgress(read, true)
	}
	return out, read, nil
}

func readJSONLFileWithProgress(path string, progressEvery int, onProgress func(read int, done bool)) ([]domain.MessageRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 * 1024 * 1024
	scanner.Buffer(make([]byte, 1024), maxLine)

	out := make([]domain.MessageRecord, 0, 1024)
	read := 0
	for scanner.Scan() {
		var m domain.MessageRecord
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			return nil, err
		}
		out = append(out, m)
		read++
		if onProgress != nil && progressEvery > 0 && read%progressEvery == 0 {
			onProgress(read, false)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if onProgress != nil {
		onProgress(read, true)
	}
	return out, nil
}

func scanMaxMessageID(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 * 1024 * 1024
	scanner.Buffer(make([]byte, 1024), maxLine)

	maxID := 0
	for scanner.Scan() {
		var row struct {
			MessageID int `json:"message_id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return 0, err
		}
		if row.MessageID > maxID {
			maxID = row.MessageID
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return maxID, nil
}

func resolveSegmentForAppend(dir string, maxPerFile int) (segment int, path string, messageCount int, err error) {
	paths, err := filepath.Glob(filepath.Join(dir, "messages_*.jsonl"))
	if err != nil {
		return 0, "", 0, err
	}
	if len(paths) == 0 {
		return 1, segmentPath(dir, 1), 0, nil
	}
	sort.Strings(paths)
	latest := paths[len(paths)-1]
	segment = parseSegmentFromPath(latest)
	if segment <= 0 {
		segment = len(paths)
	}
	messageCount, err = countJSONLLines(latest)
	if err != nil {
		return 0, "", 0, err
	}
	if maxPerFile <= 0 || messageCount < maxPerFile {
		return segment, latest, messageCount, nil
	}
	segment++
	return segment, segmentPath(dir, segment), 0, nil
}

func segmentPath(dir string, segment int) string {
	if segment <= 0 {
		segment = 1
	}
	return filepath.Join(dir, fmt.Sprintf("messages_%06d.jsonl", segment))
}

func parseSegmentFromPath(path string) int {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "messages_") || !strings.HasSuffix(base, ".jsonl") {
		return 0
	}
	v := strings.TrimPrefix(base, "messages_")
	v = strings.TrimSuffix(v, ".jsonl")
	seg, err := strconv.Atoi(v)
	if err != nil || seg <= 0 {
		return 0
	}
	return seg
}

func countJSONLLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 * 1024 * 1024
	scanner.Buffer(make([]byte, 1024), maxLine)
	count := 0
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !st.IsDir()
}
