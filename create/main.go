package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var characters = []byte("1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM")

// var characters = []byte("`1234567890-=~!@#$%^&*()_+qwertyuiop[]\\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:\"ZXCVBNM<>?")

type creator struct {
	splitter byte

	allChars bool
	charSize int32

	max  int
	size uint64

	rand *rand.Rand
	w    *bufio.Writer
}

func main() {
	now := time.Now()

	var (
		allChars  bool
		maxSize   int
		filename  string
		humanSize string
		splitter  string
	)

	flag.BoolVar(&allChars, "a", false, "use all ascii characters")
	flag.IntVar(&maxSize, "m", 10, "max length of one word")
	flag.StringVar(&filename, "f", "big-file.txt", "big file name")
	flag.StringVar(&humanSize, "s", "100M", "file size, i.g. 10G")
	flag.StringVar(&splitter, "e", "\n", "word splitter")
	flag.Parse()

	if len(splitter) == 0 || len(splitter) > 1 {
		log.Printf("invalid word splitter[%q]\n", splitter)
		return
	}

	size, err := realSize(humanSize)
	if err != nil {
		log.Println(err)
		return
	}

	if err = os.MkdirAll(filepath.Dir(filename), 0666); err != nil {
		log.Println(err)
		return
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()

	c := &creator{
		splitter: splitter[0],

		allChars: allChars,
		charSize: int32(len(characters)),

		max:  maxSize,
		size: size,

		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		w:    bufio.NewWriter(file),
	}
	if c.max <= 0 {
		c.max = 10
	}
	if c.allChars {
		c.charSize = 128
	}
	defer c.w.Flush()

	if err = c.randFile(); err != nil {
		log.Println(err)
		return
	}

	log.Printf("create[%v] size[%v] cost[%s]\n", filename, humanSize, time.Now().Sub(now))
}

func (c *creator) randByte() byte {
	var (
		char byte
		num  int32
	)
	for {
		num = c.rand.Int31n(c.charSize)
		if c.allChars {
			char = byte(num)
		} else {
			char = characters[num]
		}

		if char != c.splitter {
			break
		}
	}
	return char
}

func (c *creator) randWord() (int, error) {
	size := c.rand.Intn(c.max) + 1
	var err error
	for i := 0; i < size; i++ {
		if err = c.w.WriteByte(c.randByte()); err != nil {
			return i, err
		}
	}

	if err = c.w.WriteByte(c.splitter); err != nil {
		return size, err
	}

	return size + 1, nil
}

func (c *creator) randFile() error {
	var (
		filesize uint64
		size     int
		err      error
	)
	for filesize < c.size {
		if size, err = c.randWord(); err != nil {
			return err
		}
		filesize += uint64(size)
	}
	return nil
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
