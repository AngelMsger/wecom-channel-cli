// Package callback verifies WeCom's signed AES-CBC callback protocol.
package callback

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1" // Required by the WeCom callback protocol.
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrInvalid = errors.New("invalid callback")

type Verifier struct {
	token    string
	key      []byte
	receiver string
}

func New(token, encodedKey, receiver string) (*Verifier, error) {
	// WeCom's published key vector has non-zero unused base64 bits. Its
	// EncodingAESKey contract uses ordinary decoding, unlike ciphertext.
	key, err := base64.StdEncoding.DecodeString(encodedKey + "=")
	if err != nil || len(encodedKey) != 43 || len(key) != 32 || len(token) == 0 || len(token) > 256 {
		return nil, ErrInvalid
	}
	return &Verifier{token, key, receiver}, nil
}

// Open authenticates before decryption and exposes one indistinguishable error.
func (v *Verifier) Open(signature, timestamp, nonce, encrypted string, now time.Time) ([]byte, error) {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || ts < now.Unix()-300 || ts > now.Unix()+300 || strconv.FormatInt(ts, 10) != timestamp || len(nonce) == 0 || len(nonce) > 64 || len(encrypted) > 256<<10 {
		return nil, ErrInvalid
	}
	parts := []string{v.token, timestamp, nonce, encrypted}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, "")))
	got, err := hex.DecodeString(signature)
	if err != nil || len(signature) != 40 || !hmac.Equal(sum[:], got) {
		return nil, ErrInvalid
	}
	data, err := base64.StdEncoding.Strict().DecodeString(encrypted)
	if err != nil || len(data) == 0 || len(data)%32 != 0 {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(v.key)
	if err != nil {
		return nil, ErrInvalid
	}
	cipher.NewCBCDecrypter(block, v.key[:aes.BlockSize]).CryptBlocks(data, data)
	pad := int(data[len(data)-1])
	if pad < 1 || pad > 32 || pad > len(data) {
		return nil, ErrInvalid
	}
	for _, b := range data[len(data)-pad:] {
		if int(b) != pad {
			return nil, ErrInvalid
		}
	}
	data = data[:len(data)-pad]
	if len(data) < 20 {
		return nil, ErrInvalid
	}
	n := uint64(binary.BigEndian.Uint32(data[16:20]))
	if n > uint64(len(data)-20) {
		return nil, ErrInvalid
	}
	end := 20 + int(n)
	if !hmac.Equal(data[end:], []byte(v.receiver)) {
		return nil, ErrInvalid
	}
	return data[20:end], nil
}
