package alloydb

import (
	"fmt"
	"strconv"
)

// defaultDistanceStrategy is the default strategy used if none is provided
var defaultDistanceStrategy = cosineDistance{}

type distanceStrategy interface {
	String() string
	operator() string
	searchFunction() string
}

type euclidean struct{}

func (e euclidean) String() string {
	return "euclidean"
}
func (e euclidean) operator() string {
	return "<->"
}
func (e euclidean) searchFunction() string {
	return "vector_l2_ops"
}

type cosineDistance struct{}

func (c cosineDistance) String() string {
	return "cosineDistance"
}
func (c cosineDistance) operator() string {
	return "<=>"
}
func (c cosineDistance) searchFunction() string {
	return "vector_cosine_ops"
}

type innerProduct struct{}

func (i innerProduct) String() string {
	return "innerProduct"
}
func (i innerProduct) operator() string {
	return "<#>"
}
func (i innerProduct) searchFunction() string {
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
