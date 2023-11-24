package bloom_filter

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	xxhash "github.com/cespare/xxhash/v2"
)

type hashFunc func([]byte) []uint32

const (
	// The minimum value that a hash function may return
	HashMin = uint32(0)
	// The maximum value that a hash function may return
	HashMax = ^uint32(0)
)

// Wrappper for a defined Hash function.
type Hash struct {
	Func   hashFunc
	Name   string
	Weight int
}

// Registry for availalble Hashes
var Hashes = [...]Hash{
	{Func: fnv1a_64, Name: "fnv1a_64", Weight: 2},
	{Func: sha_256, Name: "sha_256", Weight: 8},
	{Func: xxhash_64, Name: "xxhash_64", Weight: 2},
	//{Func: fnv1a_64, Name: "fnv1a_6", Weight: 2},
	// The following hash functions fail the uniformity test
	// {Func: djb2_64, Name: "djb2_64", Weight: 1},
	// 	dictionary words: hash function djb2_64 failed two-tailed chi-squared test for uniformity
	// 		(lcv=81.45, chi^2=232_2775.23, ucv=117.41, alpha=0.2)
	// 	random strings: hash function djb2_64 failed two-tailed chi-squared test for uniformity
	// 		(lcv=81.45, chi^2=199_820.85, ucv=117.41, alpha=0.2)
}

var errNotEnoughHashFunctions = fmt.Errorf("not enough available hash functions")

func hashset(weight int) ([]Hash, error) {
	hs := []Hash{}
	w := weight
	for _, h := range Hashes {
		if w <= 0 {
			break
		}
		hs = append(hs, h)
		w -= h.Weight
	}
	if w > 0 {
		return nil, errNotEnoughHashFunctions
	}
	return hs, nil
}

const (
	djb264Start = 5381
)

// further reading on djb2 https://theartincode.stanis.me/008-djb2/
// "explanation" of djb2 magic constants https://stackoverflow.com/a/13809282
//
//lint:ignore U1000 may be used in future
func djb2_64(in []byte) []uint32 {
	h := uint64(djb264Start)
	for _, e := range in {
		h = ((h << 5) + h) + uint64(e)
	}
	return []uint32{
		uint32(h),
		// uint32(h >> 32),
	}
}

const (
	fnva64Prime  = 1099511628211
	fnva64Offset = 14695981039346656037
)

// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func fnv1a_64(in []byte) []uint32 {
	h := uint64(fnva64Offset)
	for _, e := range in {
		h ^= uint64(e)
		h *= uint64(fnva64Prime)
	}
	return []uint32{
		uint32(h),
		uint32(h >> 32),
	}
}

func sha_256(in []byte) []uint32 {
	bytes := sha256.Sum256(in)
	hashes := make([]uint32, 8)
	for n := 0; n < 8; n += 1 {
		hashes[n] = binary.NativeEndian.Uint32(bytes[n*4 : n*4+4])
	}
	return hashes
}

func xxhash_64(in []byte) []uint32 {
	h := xxhash.Sum64(in)
	return []uint32{
		uint32(h),
		uint32(h >> 32),
	}
}
