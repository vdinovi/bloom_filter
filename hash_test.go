package bloom_filter_test

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/vdinovi/go/bloom_filter"
	"github.com/vdinovi/go/streams"
)

var lowerCritVals criticalValues
var upperCritVals criticalValues

func init() {
	lowerCritVals.fromCSV("hash_test/chi_squared_lower_critical_values.csv")
	upperCritVals.fromCSV("hash_test/chi_squared_upper_critical_values.csv")
}

func TestHashFunctions(t *testing.T) {
	numBins := 20
	for _, hash := range bloom_filter.Hashes {
		numSamplesPerBin := 1000 / hash.Weight
		err := assertHashUniformity(t, hash, numBins, numSamplesPerBin)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
}

func assertHashUniformity(t *testing.T, hash bloom_filter.Hash, numBins int, numSamplesPerBin int) error {
	t.Helper()
	source, err := streams.NewRandomStringReader(5, 20)
	if err != nil {
		return err
	}
	stream, err := streams.NewWordStreamer(source)
	if err != nil {
		return err
	}

	observed, expected, err := sample(t, hash, numBins, numSamplesPerBin, stream)
	if err != nil {
		return err
	}

	if closingCh := stream.Close(); closingCh != nil {
		for err := range closingCh {
			return err
		}
	}

	alpha := alpha_10
	err = assertUniformDistribution(t, hash, observed, expected, numSamplesPerBin*numBins, numBins, alpha)
	if err != nil {
		return err
	}

	return nil
}

// two-tailed chi-squared test
func assertUniformDistribution(t *testing.T, hash bloom_filter.Hash, observed, expected []int, numSamples, degreeOfFreedom int, alpha float64) (err error) {
	t.Helper()
	chiSquared := chiSquaredTest(observed, expected)
	var lcv, ucv float64
	if lcv, err = lowerCritVals.lookup(degreeOfFreedom, alpha/2); err != nil {
		return err
	}
	if ucv, err = upperCritVals.lookup(degreeOfFreedom, 1-(alpha/2)); err != nil {
		return err
	}
	if chiSquared < lcv || chiSquared > ucv {
		t.Errorf(`
		hash function %s failed two-tailed chi-squared test for uniformity (lcv=%f, chi^2=%f, ucv=%f, alpha=%f, n=%d)
			expected=[%d, ...]
			observed=%v`,
			hash.Name, lcv, chiSquared, ucv, alpha, numSamples, expected[0], observed)
	}

	return nil
}

// https://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test
func chiSquaredTest(obs, exp []int) (result float64) {
	if len(obs) != len(exp) {
		panic(fmt.Sprintf("slice len mismatch: %d != %d", len(obs), len(exp)))
	}
	for i, o := range obs {
		e := exp[i]
		if o == 0 && e == 0 {
			continue
		}
		a := float64(o)
		b := float64(e)
		result += ((a - b) * (a - b)) / b
	}
	return result
}

func sample(t *testing.T, hash bloom_filter.Hash, numBins, numSamplesPerBin int, stream *streams.WordStreamer) (observed, expected []int, err error) {
	t.Helper()
	binSize := (bloom_filter.HashMax - bloom_filter.HashMin) / uint32(numBins)
	binRanges := make([]uint32, numBins+1)
	for i := 0; i < numBins; i += 1 {
		binRanges[i] = uint32(i) * binSize
	}
	binRanges[len(binRanges)-1] = bloom_filter.HashMax
	numSamples := numBins * numSamplesPerBin
	expected = make([]int, numBins)
	for i := range expected {
		expected[i] = numSamplesPerBin * hash.Weight
	}
	observed = make([]int, numBins)
	bin := func(v uint32) int {
		for i := 1; i < numBins; i += 1 {
			if v < binRanges[i] {
				return i - 1
			}
		}
		return numBins - 1
	}
	for i := 0; i < numSamples; i += 1 {
		select {
		case err = <-stream.Errors():
			return nil, nil, err
		case word := <-stream.Words():
			//t.Log(word)
			for _, v := range hash.Func([]byte(word)) {
				b := bin(v)
				if b < 0 {
					return nil, nil, fmt.Errorf("no bin found for value %d", v)
				}
				observed[b] += 1
			}
		}
	}
	return observed, expected, nil
}

const (
	alpha_20  = 0.2
	alpha_10  = 0.1
	alpha_05  = 0.05
	alpha_02  = 0.02
	alpha_002 = 0.002
)

// https://www.itl.nist.gov/div898/handbook/eda/section3/eda3674.htm
type criticalValues struct {
	probs []float64
	table map[int][]float64
}

func (c *criticalValues) lookup(degFreedom int, prob float64) (float64, error) {
	var col int
	for col = 0; col < len(c.probs); col += 1 {
		if c.probs[col] == prob {
			break
		}
	}
	if col == len(c.probs) {
		return 0, fmt.Errorf("prob %f not found", prob)
	}
	row, ok := c.table[degFreedom]
	if !ok {
		return 0, fmt.Errorf("degree of freedom %d not found", degFreedom)
	}
	return row[col], nil
}

func (c *criticalValues) fromCSV(path string) error {
	temp := criticalValues{}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	probs, err := r.Read()
	if err != nil {
		return err
	}
	probs = probs[1:]
	temp.probs = make([]float64, len(probs))
	for i := 0; i < len(probs); i += 1 {
		temp.probs[i], err = strconv.ParseFloat(probs[i], 64)
		if err != nil {
			return err
		}
	}
	table, err := r.ReadAll()
	if err != nil {
		return err
	}
	temp.table = make(map[int][]float64, len(table))
	for _, row := range table {
		v, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return err
		}
		vals := make([]float64, len(probs))
		for i, n := range row[1:] {
			vals[i], err = strconv.ParseFloat(n, 64)
			if err != nil {
				return err
			}
		}
		temp.table[int(v)] = vals
	}
	*c = temp
	return nil
}
