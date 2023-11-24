package bloom_filter_test

import (
	_ "embed"
	"math"
	"testing"

	"github.com/vdinovi/go/bloom_filter"
	"github.com/vdinovi/go/streams"
)

const uint32max = int(^uint32(0))

func TestBloomFilterBasic(t *testing.T) {
	f, err := bloom_filter.NewBloomFilter(32, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"abc", "def", "ghi"} {
		b := []byte(w)
		f.Add(b)
		if !f.Query(b) {
			t.Errorf("expected %q to be in set but was not: %s", w, f.Display(true))
		}
	}
	for _, w := range []string{"jkl", "mno", "pqr"} {
		b := []byte(w)
		if f.Query(b) {
			t.Errorf("didn't expected %q to be in set but was: %s", w, f.Display(true))
		}
	}

}

func TestFilterErrors(t *testing.T) {
	tests := []struct {
		size      int
		numHashes int
		shouldErr bool
		numBits   int
	}{
		{-1, 2, true, 0},
		{0, 2, true, 0},
		{1, 2, false, 8},
		{2, 2, false, 8},
		{8, 2, false, 8},
		{9, 2, false, 16},
		{10, 2, false, 16},
		{16, 2, false, 16},
		{17, 2, false, 24},
		{18, 2, false, 24},
		{uint32max, 2, false, 4294967296},
		{uint32max + 1, 2, false, 4294967296},
		{uint32max + 2, 2, true, 0},
		{1, -1, true, 0},
		{1, 0, true, 0},
		{1, 1, true, 0},
		{1, 2, false, 8},
		{1, 3, false, 8},
		{1, 4, false, 8},
		{1, 13, true, 0},
	}
	for _, test := range tests {
		f, err := bloom_filter.NewBloomFilter(test.size, test.numHashes)
		if test.shouldErr {
			if err == nil {
				t.Error("expected error but got none")
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if test.numBits != f.NumBits() {
			t.Fatalf("expected NewBloomFilter(%d, %d) to return filter with %d bytes, but got %d",
				test.size, test.numHashes, test.numBits, f.NumBits())
		}
	}
}

func TestBloomFilter(t *testing.T) {
	source, err := streams.NewRandomStringReader(5, 10)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	stream, err := streams.NewWordStreamer(source)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	hashWeight := 6
	count := 1000
	numBits := 24729
	f, err := bloom_filter.NewBloomFilter(numBits, hashWeight)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	cm := &confusionMatrix{}
	var entries map[string]bool
	if entries, err = populate(count, f, stream, cm); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := probe(count*1000, f, stream, entries, cm); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if closingCh := stream.Close(); closingCh != nil {
		for err := range closingCh {
			t.Errorf("unexpected error: %s", err)
		}
	}

	expectedFPR := bloom_filter.ExpectedFalsePositiveRate(count, f.NumBits(), f.HashWeight())
	actualFPR := 1 - cm.specificity()
	threshold := 0.00001
	if diff := math.Abs(expectedFPR - actualFPR); diff > threshold {
		t.Errorf(
			"Expected false positivity rate of %f but got %f (diff=%f)\n . filter=%s\n . entries=%d\n . accuracy=%f\n . sensitivity=%f\n . specificity=%f\n . cm=%+v",
			expectedFPR, actualFPR, diff, f, count, cm.accuracy(), cm.sensitivity(), cm.specificity(), cm)
	}
}

func populate(count int, f *bloom_filter.BloomFilter, stream *streams.WordStreamer, cm *confusionMatrix) (entries map[string]bool, err error) {
	entries = make(map[string]bool, count)
	for i := 0; i < count; i += 1 {
		select {
		case err = <-stream.Errors():
			return nil, err
		case word := <-stream.Words():
			entries[word] = true
			f.Add([]byte([]byte(word)))
		}
	}
	for word := range entries {
		if f.Query([]byte(word)) {
			cm.TruePos += 1
		} else {
			cm.FalseNeg += 1
		}
	}
	return entries, nil
}

func probe(count int, f *bloom_filter.BloomFilter, stream *streams.WordStreamer, entries map[string]bool, cm *confusionMatrix) (err error) {
	for i := 0; i < count; i += 1 {
		select {
		case err = <-stream.Errors():
			return err
		case word := <-stream.Words():
			if f.Query([]byte(word)) {
				if entries[word] {
					cm.TruePos += 1
				} else {
					cm.FalsePos += 1
				}
			} else {
				if entries[word] {
					cm.FalseNeg += 1
				} else {
					cm.TrueNeg += 1
				}
			}
		}
	}
	return nil
}

type confusionMatrix struct {
	TruePos  int
	TrueNeg  int
	FalsePos int
	FalseNeg int
}

func (cm confusionMatrix) accuracy() float64 {
	return float64(cm.TruePos+cm.TrueNeg) / float64(cm.TruePos+cm.TrueNeg+cm.FalsePos+cm.FalseNeg)
}

// a.k.a True Positive Rate
func (cm confusionMatrix) sensitivity() float64 {
	return float64(cm.TruePos) / float64(cm.TruePos+cm.FalseNeg)
}

// a.k.a True Negative Rate
func (cm confusionMatrix) specificity() float64 {
	return float64(cm.TrueNeg) / float64(cm.TrueNeg+cm.FalsePos)
}
