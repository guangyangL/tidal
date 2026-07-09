package leaderboard

type RankConfig struct {
	MaxScore     int
	BucketNumber int
	TopK         int
}

func NewRankConfig(maxScore, bucketNumber, topK int) *RankConfig {
	return &RankConfig{
		MaxScore:     maxScore,
		BucketNumber: bucketNumber,
		TopK:         topK,
	}
}
