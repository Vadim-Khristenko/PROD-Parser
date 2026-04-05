package domain

type SearchHit struct {
	InternalID uint64  `json:"internal_id"`
	Score      float64 `json:"score"`
}

type SearchResult struct {
	Hit     SearchHit     `json:"hit"`
	Message MessageRecord `json:"message"`
}
