package alloydb

import (
	"fmt"
	"strconv"
)

// defaultDistanceStrategy is the default strategy used if none is provided
var defaultDistanceStrategy = CosineDistance{}

type distanceStrategy interface {
	String() string
	operator() string
	searchFunction() string
}

type Euclidean struct{}

func (e Euclidean) String() string {
	return "euclidean"
}
func (e Euclidean) operator() string {
	return "<->"
}
func (e Euclidean) searchFunction() string {
	return "vector_l2_ops"
}

type CosineDistance struct{}

func (c CosineDistance) String() string {
	return "cosineDistance"
}
func (c CosineDistance) operator() string {
	return "<=>"
}
func (c CosineDistance) searchFunction() string {
	return "vector_cosine_ops"
}

type InnerProduct struct{}

func (i InnerProduct) String() string {
	return "innerProduct"
}
func (i InnerProduct) operator() string {
	return "<#>"
}
func (i InnerProduct) searchFunction() string {
	return "vector_ip_ops"
}

// indexOptions returns the specific options for the index based on the index type
func (index *BaseIndex) indexOptions(indexOpts []int) string {
	switch index.indexType {
	case "hnsw":
		{
			m := 16
			ef_construction := 64
			if len(indexOpts) == 2 {
				m = indexOpts[0]
				ef_construction = indexOpts[1]
			}
			return fmt.Sprintf("(m = %s, ef_construction = %s)", strconv.Itoa(m), strconv.Itoa(ef_construction))
		}
	case "ivfflat":
		{
			lists := 100
			if len(indexOpts) == 1 {
				lists = indexOpts[0]
			}
			return fmt.Sprintf("(lists = %s)", strconv.Itoa(lists))
		}
	case "ivf":
		{
			lists := 100
			if len(indexOpts) == 1 {
				lists = indexOpts[0]
			}
			return fmt.Sprintf("(lists = %s, quantizer = sq8)", strconv.Itoa(lists))
		}
	case "ScaNN":
		{
			numLeaves := 5
			if len(indexOpts) == 1 {
				numLeaves = indexOpts[0]
			}
			return fmt.Sprintf("(num_leaves = %s, quantizer = sq8)", strconv.Itoa(numLeaves))
		}
	default:
		return ""
	}
}
