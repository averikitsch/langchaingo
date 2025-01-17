package alloydb

type distanceStrategy int

const (
	euclidean distanceStrategy = iota
	cosineDistance
	innerProduct
)

// defaultDistanceStrategy is the default strategy used if none is provided
var defaultDistanceStrategy = cosineDistance

// String method to display distanceStrategy as string
func (ds distanceStrategy) string() string {
	switch ds {
	case euclidean:
		return "euclidean"
	case cosineDistance:
		return "cosineDistance"
	case innerProduct:
		return "innerProduct"
	default:
		return "Unknown"
	}
}

// operator returns the operator used by the distanceStrategy
func (ds distanceStrategy) operator() string {
	switch ds {
	case euclidean:
		return "<->"
	case cosineDistance:
		return "<=>"
	case innerProduct:
		return "<#>"
	default:
		return ""
	}
}

// searchFunction returns the appropriate search function for the distanceStrategy
func (ds distanceStrategy) searchFunction() string {
	switch ds {
	case euclidean:
		return "l2_distance"
	case cosineDistance:
		return "cosine_distance"
	case innerProduct:
		return "inner_product"
	default:
		return ""
	}
}

// indexOptions returns the specific options for the index based on the index type
func (index *BaseIndex) indexOptions() string {
	switch index.indexType {
	case "hnsw":
		return "(m = 16, ef_construction = 64)"
	case "ivfflat":
		return "(lists = 100)"
	case "ivf":
		return "(lists = 100, quantizer = sq8)"
	case "ScaNN":
		return "(num_leaves = 5, quantizer = sq8)"
	default:
		return ""
	}
}
