package leaderboard

import "fmt"

type SegmentNode struct {
	lower, upper int
	left, right  *SegmentNode
	count        int
}

func BuildSegmentTree(conf *RankConfig) *SegmentNode {
	return buildSegmentTree(conf.MaxScore, conf.BucketNumber)
}

func buildSegmentTree(maxScore, bucketNumber int) *SegmentNode {
	segLen := (maxScore + bucketNumber - 1) / bucketNumber
	var leaves []*SegmentNode
	for i := 0; i < maxScore; i += segLen {
		upper := i + segLen - 1
		if upper > maxScore {
			upper = maxScore
		}
		leaves = append(leaves, &SegmentNode{
			lower: i,
			upper: upper,
		})
	}
	cur := leaves
	for len(cur) > 1 {
		var next []*SegmentNode
		for i := 0; i < len(cur); i += 2 {
			if i+1 < len(cur) {
				next = append(next, &SegmentNode{
					lower: cur[i].lower,
					upper: cur[i+1].upper,
					left:  cur[i],
					right: cur[i+1],
				})
			} else {
				next = append(next, cur[i])
			}
		}
		cur = next
	}
	return cur[0]
}

func findSegment(node *SegmentNode, score int) *SegmentNode {
	if node == nil || score < node.lower || score > node.upper {
		return nil
	}
	if node.left == nil {
		return node
	}
	if score <= node.left.upper {
		return findSegment(node.left, score)
	}
	return findSegment(node.right, score)
}

func getSegmentsOnPath(root *SegmentNode, score int) []string {
	var segs []string
	for node := root; node != nil; {
		if node.lower > score || node.upper < score {
			break
		}
		segs = append(segs, nodeKey(node.lower, node.upper))
		if node.left == nil {
			break
		}
		if score <= node.left.upper {
			node = node.left
		} else {
			node = node.right
		}
	}
	return segs
}

func nodeKey(lower, upper int) string {
	return fmt.Sprintf("%d-%d", lower, upper)
}
