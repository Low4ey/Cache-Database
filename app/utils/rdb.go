package utils

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"os"
)

const (
	OPCODE_AUX            = 0xFA
	OPCODE_RESIZE_DB      = 0xFB
	OPCODE_EXPIRE_TIME_MS = 0xFC
	OPCODE_EXPIRE_TIME    = 0xFD
	OPCODE_SELECT_DB      = 0xFE
	OPCODE_EOF            = 0xFF
)

type RdbFile struct {
	data   []byte
	cursor int
}

func (f *RdbFile) consumeByte() byte {
	val := f.data[f.cursor]
	f.cursor += 1
	return val
}

func (f *RdbFile) unreadByte() {
	f.cursor -= 1
}

func (f *RdbFile) consumeSlice(length int) []byte {
	bytes := f.data[f.cursor:(f.cursor + length)]
	f.cursor += length
	return bytes
}
func (f *RdbFile) parseRdbLength() int {
	var length int
	b := f.consumeByte()
	switch (b & 0b1100_0000) >> 6 {
	case 0b00:
		length = int(b & 0b0011_1111)
	case 0b01:
		next_byte := f.consumeByte()
		length = (int(b&0b0011_1111) << 8) | int(next_byte)
	case 0b10:
		length = int(f.consumeByte()) << 24
		length |= int(f.consumeByte()) << 16
		length |= int(f.consumeByte()) << 8
		length |= int(f.consumeByte()) << 0
	case 0b11:
		switch b & 0b0011_1111 {
		case 0:
			length = 1
		case 1:
			length = 2
		case 2:
			length = 4
		case 3:
			panic("Compressed string no yet implemente")
		default:
			panic(fmt.Sprintf("Unkown speccial case encoding: %d", b&0b0011_1111))
		}
	}
	return length
}

func (f *RdbFile) parseRdbString() string {
	length := f.parseRdbLength()
	s := string(f.consumeSlice(length))
	return s
}

func (f *RdbFile) parseRdbKeyValuePair() (string, string) {
	valueType := f.consumeByte()
	key := f.parseRdbString()
	var value string
	switch valueType {
	case 0:
		value = f.parseRdbString()
	default:
		panic(fmt.Sprintf("Value type not yet implemented: %b", valueType))
	}
	return key, value
}

func readFile(path string) (RdbFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RdbFile{nil, 0}, err
	}
	return RdbFile{data, 0}, nil
}

func ReadRdbFile(dir, dbfilename string) error {
	file, err := readFile(dir + "/" + dbfilename)
	if err != nil {
		return err
	}
	signatue := file.consumeSlice(5)
	if string(signatue) != "REDIS" {
		return fmt.Errorf("rdb file doe not start with 'REDIS' magic string, got: %s", string(signatue))
	}
	_ = file.consumeSlice(4)
outer:
	for {
		b := file.consumeByte()
		switch b {
		case OPCODE_AUX:
			_ = file.parseRdbString()
			_ = file.parseRdbString()
		case OPCODE_SELECT_DB:
			file.consumeByte()
		case OPCODE_RESIZE_DB:
			_ = file.parseRdbLength()
			_ = file.parseRdbLength()
		case OPCODE_EXPIRE_TIME:
			expiry := binary.LittleEndian.Uint64(file.consumeSlice(8)) // Use 8 bytes for a uint64
			expiredTime := time.Unix(int64(expiry), 0)
			key, value := file.parseRdbKeyValuePair()
			log.Println(key, value, expiredTime)
			if time.Now().After(expiredTime) {
				continue
			}
			rdb[key] = value
			durationUntilExpiration := expiredTime.Sub(time.Now())
			time.AfterFunc(durationUntilExpiration, func() {
				delete(rdb, key)
			})

		case OPCODE_EXPIRE_TIME_MS:
			expiry := binary.LittleEndian.Uint64(file.consumeSlice(8))
			expiredTime := time.UnixMilli(int64(expiry))
			key, value := file.parseRdbKeyValuePair()
			log.Println(key, value, expiry)
			if time.Now().After(expiredTime) {
				continue
			}
			rdb[key] = value
		case OPCODE_EOF:
			break outer
		default:
			file.unreadByte()
			key, value := file.parseRdbKeyValuePair()
			rdb[key] = value
		}
	}
	return nil
}
