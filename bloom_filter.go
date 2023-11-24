// A bloom filter is a probabalistic data structure for representing sets
// read more at https://en.wikipedia.org/wiki/Bloom_filter
package bloom_filter

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

// A Bloom Filter
// as described in https://en.wikipedia.org/wiki/Bloom_filter
type BloomFilter struct {
	bitset  []byte // M bits
	hashset []Hash // K Hash functions
}

const (
	minSize       = 1
	maxSize       = int(^uint32(0)) + 1
	minHashWeight = 2
)

var (
	errSizeTooSmall = fmt.Errorf("size must be greater than %d", minSize-1)
	errSizeTooLarge = fmt.Errorf("size must be less than %d", maxSize+1)
	errTooFewHashes = fmt.Errorf("hash weight must be greater than %d", minHashWeight-1)
)

// Constructs a BloomFilter with the specified size and hashweight
//
// size refers to the expected size of elements in the set. This informs the selection
// of size for the underlying bitset which has an impact of the false positivity rate.
//
// hash weight refers to the number of hashes used. Since a hash may yield multiple
// values which span the bitrange (ex: sha256 -> 8 uint32s), some hash functions carry
// more "weight" than others. Increasing this may provide better specificity at the cost
// of potentially more hash computation.
func NewBloomFilter(size int, hashWeight int) (f *BloomFilter, err error) {
	if size < minSize {
		return nil, errSizeTooSmall
	}
	if size > maxSize {
		return nil, errSizeTooLarge
	}
	if hashWeight < minHashWeight {
		return nil, errTooFewHashes
	}
	f = &BloomFilter{}
	if f.hashset, err = hashset(hashWeight); err != nil {
		return nil, err
	}
	f.bitset = make([]byte, int(math.Ceil(float64(size)/8)))
	return f, nil
}

// Returns the HashWeight. See NewBloomFilter for details
func (f *BloomFilter) HashWeight() (result int) {
	for _, h := range f.hashset {
		result += h.Weight
	}
	return result
}

// Returns the number of bits used. This is proportional to the specified size during construction.
func (f *BloomFilter) NumBits() int {
	return len(f.bitset) * 8
}

// Adds an element to the set
func (f *BloomFilter) Add(e []byte) {
	for _, hash := range f.hashset {
		for _, h := range hash.Func(e) {
			h = f.wrap(h)
			f.set(h)
		}
	}
}

// Queries for an element in the set.
// Returns true if the element may, but is not necessarily, in the set.
// Returns false if the element is not in the set.
func (f *BloomFilter) Query(e []byte) bool {
	for _, hash := range f.hashset {
		for _, h := range hash.Func(e) {
			h = f.wrap(h)
			if !f.check(h) {
				return false
			}
		}
	}
	return true

}

func (f *BloomFilter) String() string {
	return f.Display(false)
}

// Displays the bloom filter
// unless `showBitset“ is specified, this is filtered from the output
func (f *BloomFilter) Display(showBitset bool) string {
	hashWeight := 0
	hsNames := make([]string, len(f.hashset))
	for i, h := range f.hashset {
		hashWeight += h.Weight
		hsNames[i] = h.Name
	}
	hs := strings.Join(hsNames, ",")
	bs := "filtered"
	if showBitset {
		bs = hex.EncodeToString(f.bitset)
	}
	return fmt.Sprintf("BloomFilter{numBits=%d, hashWeight=%d, hashset=[%s], bitset=[%s]}",
		len(f.bitset)*8, f.HashWeight(), hs, bs)
}

func (f *BloomFilter) set(h uint32) {
	byteIndex := uint32(len(f.bitset)) - 1 - uint32(h/8)
	bitOffset := uint8(h % 8)
	f.bitset[byteIndex] |= 1 << bitOffset
}

func (f *BloomFilter) check(h uint32) bool {
	byteIndex := uint32(len(f.bitset)) - 1 - uint32(h/8)
	bitOffset := uint8(h % 8)
	return f.bitset[byteIndex]&(1<<bitOffset) != 0
}

func (f *BloomFilter) wrap(n uint32) uint32 {
	return n % uint32(len(f.bitset)*8)
}

// Calculates the probability that query returns a false positive
// after n elements have been added to the set with k number of hash functions.
//
// Optimized values parameterized by error rate are:
//
//	k ~ -log2(err)
//	m/n ~ -1.44 * log2(err)
//
// For 2% error, choose k ~ 5.643, m/n ~ 8.127
//
// https://hur.st/bloomfilter
func ExpectedFalsePositiveRate(numItems, numBits, numHashes int) float64 {
	n := float64(numItems)
	m := float64(numBits)
	k := float64(numHashes)

	// Original Calculation
	//   m -> num bits
	//   1/m -> prob bit set
	//   1 - 1/m -> prob bit not set
	//   (1 - 1/m)^kn -> prob bit not set for k hashes across n items
	//	 1 - ((1 - 1/m)^kn) -> prob bit set for k hashes across n items
	//	 (1 - ((1 - 1/m)^kn))^k -> prob bit set for k hashes across n items for a new item (all of k)
	// Which can be simplified down to
	//   (1 - (e^(-kn/m))^k
	return math.Pow(1-math.Exp(-k/(m/n)), k)
}

func bitstringUint32(h uint32) string {
	buf := bytes.Buffer{}
	if err := binary.Write(&buf, binary.NativeEndian, h); err != nil {
		panic(err)
	}
	return bitstring(buf.Bytes())
}

func bitstring(bytes []byte) string {
	s := make([]rune, len(bytes)*8)
	for i, b := range bytes {
		for j := 0; j < 8; j += 1 {
			if 1<<j&b == 0 {
				s[i*8+j] = '1'
			} else {
				s[i*8+j] = '0'
			}
		}
	}
	return string(s)
}

func hexstringUint32(h uint32) string {
	buf := bytes.Buffer{}
	if err := binary.Write(&buf, binary.NativeEndian, h); err != nil {
		panic(err)
	}
	return hexstring(buf.Bytes())
}

func hexstring(bytes []byte) string {
	buf := make([]byte, hex.EncodedLen(len(bytes)))
	hex.Encode(buf, bytes)
	return string(buf)
}
