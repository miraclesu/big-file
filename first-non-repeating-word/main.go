package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	chunkSize int
	limitMem  uint64
)

type finder struct {
	needFlush bool
	splitter  byte
	prefix    byte

	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	writer *bufio.Writer
	reader *bufio.Reader

	word     string
	position uint64

	file   string
	fileIO *os.File

	buf []byte
	fs  map[byte]*finder

	// send word & position
	ch chan interface{}

	wordsCounter
}

func (f *finder) clone(prefix byte) (*finder, error) {
	var (
		err    error
		wc     wordsCounter
		fileIO *os.File

		file = f.file + "." + strconv.Itoa(int(prefix))
	)

	if !f.needFlush {
		wc = make(wordsCounter, 32)
	} else {
		fileIO, err = os.Create(file)
	}
	if err != nil {
		return nil, err
	}

	cf := &finder{
		splitter: f.splitter,
		prefix:   prefix,

		ctx:    f.ctx,
		cancel: f.cancel,

		file:   file,
		fileIO: fileIO,

		buf: make([]byte, 5),
		fs:  make(map[byte]*finder, 16),

		ch: make(chan interface{}, 8),

		wordsCounter: wc,
	}

	if f.needFlush {
		cf.writer = bufio.NewWriterSize(fileIO, int(chunkSize))
	}

	return cf, nil
}

func (f *finder) close() {
	if f.fileIO != nil {
		f.fileIO.Close()
	}

	if f.wordsCounter != nil {
		f.wordsCounter = nil
	}
	if f.fs != nil {
		f.fs = nil
	}

	// not the root
	if f.ch != nil {
		os.Remove(f.file)
	}
}

func (f *finder) dispatch() error {
	var (
		size     int
		position uint64
		fileSize float64

		output  bool
		current int
		num     int

		err  error
		word []byte

		wc = make(wordsCounter, 32)
	)

	defer func() {
		if f.fileIO != nil {
			f.fileIO.Close()
			f.fileIO = nil
		}
	}()

	// root finder
	if f.ch == nil {
		if info, e := f.fileInfo(); e == nil {
			fileSize = float64(info.Size())
		}
		output = fileSize > 0
	}

	for {
		select {
		case <-f.ctx.Done():
			return nil
		default:
		}

		position, word, err = f.readWord(position)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		size = len(word)
		if output {
			current += size + 1
			if current/chunkSize != num {
				num = current / chunkSize
				log.Printf("file[%s] dispatch progress: %.2f%%\n", f.file, float64(100*current)/fileSize)
			}
		}

		// drop empty string
		if size == 0 {
			continue
		}

		// TODO adjustment based on memory limitations
		// the word is short enough to count
		if size < 5 {
			wc.count(word, position)
			continue
		}

		sub := f.fs[word[0]]
		if sub == nil {
			if sub, err = f.clone(word[0]); err != nil {
				f.cancel()
				return err
			}

			f.fs[word[0]] = sub
			f.wg.Add(1)
			go func(sub *finder) {
				f.count(sub)
				f.wg.Done()
			}(sub)
		}

		sub.ch <- word
		sub.ch <- position
	}

	if output {
		log.Printf("file[%s] dispatch progress: 100%%\n", f.file)
	}

	f.getResultFromCounter(wc)
	wc = nil
	return nil
}

func (f *finder) count(sub *finder) {
	var (
		ok       bool
		isIndex  bool
		word     []byte
		err      error
		line     interface{}
		position uint64
	)

LOOP:
	for {
		select {
		case line, ok = <-sub.ch:
			if !ok {
				break LOOP
			}

			if !isIndex {
				word, ok = line.([]byte)
				if !ok {
					log.Printf("invalid line[%+v], the item isn't []byte", line)
					continue
				}

				isIndex = true
				continue
			}

			position, ok = line.(uint64)
			if !ok {
				log.Printf("invalid line[%+v], the item isn't uint64", line)
				continue
			}

			if err = sub.countOrWrite(word, position); err != nil {
				f.cancel()
				log.Printf("countOrWrite err: %s\n", err)
				return
			}
			isIndex = false
		case <-f.ctx.Done():
			return
		}
	}

	sub.getResult()
}

func (f *finder) nonRepeatingWord() error {
	defer f.close()
	if err := f.dispatch(); err != nil {
		return err
	}

Loop:
	for _, sub := range f.fs {
		select {
		case <-f.ctx.Done():
			break Loop
		default:
		}
		close(sub.ch)
	}

	f.wait()
	f.merge()
	return nil
}

func (f *finder) wait() {
	f.wg.Wait()
	if !f.needFlush {
		return
	}

	var progress int
Loop:
	for _, sub := range f.fs {
		select {
		case <-f.ctx.Done():
			break Loop
		default:
		}
		sub.runSubCount()
		// sub finder exec one by one
		// or open too many files, OOM
		sub.wg.Wait()
		progress++
		if f.ch == nil {
			log.Printf("file[%s] count prefix[%c] words progress: %.2f%%\n", f.file, sub.prefix, float64(progress*100)/float64(len(f.fs)))
		}
	}
}

func (f *finder) merge() {
Loop:
	for _, sub := range f.fs {
		select {
		case <-f.ctx.Done():
			break Loop
		default:
		}

		if sub.word == "" {
			continue
		}

		if sub.position < f.position || f.word == "" {
			f.word, f.position = sub.word, sub.position
		}
	}

	if f.word != "" {
		f.word = string(f.prefix) + f.word
	}
}

func (f *finder) countOrWrite(word []byte, position uint64) error {
	if f.wordsCounter != nil {
		// exclude prefix
		f.wordsCounter.count(word[1:], position)
		return nil
	}

	return f.writeTo(word[1:], position)
}

func (f *finder) writeTo(word []byte, position uint64) error {
	var err error
	f.buf[0] = byte(position)
	f.buf[1] = byte(position >> 8)
	f.buf[2] = byte(position >> 16)
	f.buf[3] = byte(position >> 24)
	f.buf[4] = byte(position >> 32)
	if _, err = f.writer.Write(f.buf[:5]); err != nil {
		return err
	}
	if _, err = f.writer.Write(word); err != nil {
		return err
	}
	return f.writer.WriteByte(f.splitter)
}

func (f *finder) readWord(position uint64) (uint64, []byte, error) {
	if f.ch == nil {
		word, err := f.reader.ReadBytes(f.splitter)
		if err != nil {
			return 0, nil, err
		}
		return position + 1, word[:len(word)-1], nil
	}

	_, err := f.reader.Read(f.buf[:5])
	if err != nil {
		return 0, nil, err
	}
	word, err := f.reader.ReadBytes(f.splitter)
	if err != nil {
		return 0, nil, err
	}
	position = uint64(f.buf[0]) | uint64(f.buf[1])<<8 | uint64(f.buf[2])<<16 | uint64(f.buf[3])<<24 |
		uint64(f.buf[4])<<32
	return position, word[:len(word)-1], nil
}

func (f *finder) getResultFromCounter(wc wordsCounter) {
	if len(wc) == 0 {
		return
	}

	word, position := wc.firstNonRepeat()
	if word == "" {
		return
	}

	if f.word == "" || position < f.position {
		f.word, f.position = word, position
	}
	return
}

func (f *finder) getResult() {
	if f.wordsCounter != nil {
		f.getResultFromCounter(f.wordsCounter)
		if f.word != "" {
			f.word = string(f.prefix) + f.word
		}
		f.wordsCounter = nil
		return
	}

	f.writer.Flush()
	f.writer = nil
	f.fileIO.Close()
	f.fileIO = nil
}

func (f *finder) runSubCount() {
	err := f.setNeedFlush()
	if err != nil {
		f.cancel()
		log.Printf("setNeedFlush err: %s\n", err)
		return
	}
	f.fileIO, err = os.Open(f.file)
	if err != nil {
		f.cancel()
		log.Printf("sub Open err: %s\n", err)
		return
	}
	f.reader = bufio.NewReaderSize(f.fileIO, chunkSize)

	if err = f.nonRepeatingWord(); err != nil {
		f.cancel()
		log.Printf("sub nonRepeatingWord err: %s\n", err)
		return
	}
}

func (f *finder) fileInfo() (os.FileInfo, error) {
	fileInfo, err := os.Stat(f.file)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func (f *finder) setNeedFlush() error {
	fileInfo, err := f.fileInfo()
	if err != nil {
		return err
	}

	// memory usage is about 6x the file size
	// NOTE: rough estimate
	f.needFlush = uint64(fileInfo.Size()*6) > limitMem
	return nil
}

// counter stores position and repeating flag
// the highest bit stores repeating flag
type counter [5]byte

func newCounter(position uint64) counter {
	var c counter
	c[0] = byte(position)
	c[1] = byte(position >> 8)
	c[2] = byte(position >> 16)
	c[3] = byte(position >> 24)
	v := byte(position >> 32)
	if v >= (1 << 7) {
		panic(fmt.Sprintf("too big position: %d to handle, max position limit: %d", position, 1<<39-1))
	}
	c[4] = v
	return c
}

func (c *counter) markRepeat() {
	c[4] |= 1 << 7
}

func (c *counter) repeat() bool {
	return c[4]>>7 == 1
}

func (c *counter) position() uint64 {
	return uint64(c[0]) | uint64(c[1])<<8 | uint64(c[2])<<16 | uint64(c[3])<<24 |
		uint64(c[4])<<31
}

type wordsCounter map[string]counter

func (w wordsCounter) count(word []byte, position uint64) {
	counter, ok := w[string(word)]
	if !ok {
		w[string(word)] = newCounter(position)
		return
	}

	if counter.repeat() {
		return
	}

	counter.markRepeat()
	w[string(word)] = counter
}

func (w wordsCounter) firstNonRepeat() (string, uint64) {
	if len(w) == 0 {
		return "", 0
	}

	var (
		firstWord string
		position  uint64
	)

	for word, count := range w {
		if !count.repeat() && (count.position() < position || firstWord == "") {
			firstWord, position = word, count.position()
		}
	}
	return firstWord, position
}

func main() {
	now := time.Now()

	var (
		filename string
		splitter string

		chunkSizeHuman string
		limitMemHuman  string
	)

	flag.StringVar(&filename, "f", "big-file.txt", "big file name")
	flag.StringVar(&splitter, "s", "\n", "word splitter")
	flag.StringVar(&chunkSizeHuman, "cs", "10M", "the size of read the file in chunks")
	flag.StringVar(&limitMemHuman, "l", "16G", "the size of memory limit")
	flag.Parse()

	if len(splitter) == 0 || len(splitter) > 1 {
		log.Printf("invalid word splitter[%q]\n", splitter)
		return
	}

	f := &finder{
		splitter: splitter[0],

		file: filename,
		buf:  make([]byte, 5),

		fs: make(map[byte]*finder, 256),
	}
	f.ctx, f.cancel = context.WithCancel(context.Background())

	fileInfo, err := f.fileInfo()
	if err != nil {
		log.Println(err)
		return
	}
	if fileInfo.Size() >= (512 << 30) {
		log.Printf("file[%v] size[%fG] is too big to handle, file size must less than 512G",
			filename, float64(fileInfo.Size())/(1<<30))
		return
	}

	chunck, err := realSize(chunkSizeHuman)
	if err != nil {
		log.Println(err)
		return
	}
	chunkSize = int(chunck)

	limitMem, err = realSize(limitMemHuman)
	if err != nil {
		log.Println(err)
		return
	}

	f.fileIO, err = os.Open(f.file)
	if err != nil {
		log.Println(err)
		return
	}

	f.setNeedFlush()
	f.reader = bufio.NewReaderSize(f.fileIO, chunkSize)

	done := make(chan struct{}, 1)
	go func() {
		if err = f.nonRepeatingWord(); err != nil {
			log.Println(err)
		}
		close(done)
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	select {
	case n := <-sc:
		f.cancel()
		log.Printf("receive signal %v, closing", n)
	case <-f.ctx.Done():
		log.Printf("context is done with %v, closing", f.ctx.Err())
	case <-done:
	}

	<-done
	log.Printf("the first non-repeating word in[%s] is[%s], position[%v], cost[%s]\n",
		filename, f.word, f.position, time.Now().Sub(now))

}

func realSize(humanSize string) (uint64, error) {
	count := len(humanSize)
	if count == 0 {
		return 0, fmt.Errorf("empty size")
	}

	var (
		size uint64
		i    int
	)
	for i = 0; i < count; i++ {
		if '9' < humanSize[i] || humanSize[i] < '0' {
			break
		}
		size = size*10 + uint64(humanSize[i]-'0')
	}

	if i == count {
		return size, nil
	}

	var unit uint64
	switch strings.ToUpper(humanSize[i : i+1]) {
	case "B":
	case "K":
		unit = 10
	case "M":
		unit = 20
	case "G":
		unit = 30
	case "T":
		unit = 40
	default:
		return 0, fmt.Errorf("invalid size unit[%v]", humanSize[i:])
	}

	return size * (1 << unit), nil
}
