package store

type TaskInfos []*TaskInfo

func (s TaskInfos) Len() int {
	return len(s)
}
func (s TaskInfos) Less(i, j int) bool {
	return s[i].UpdatedAt < s[j].UpdatedAt
}

func (s TaskInfos) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type TaskInfosByCreatedAt []*TaskInfo

func (s TaskInfosByCreatedAt) Len() int {
	return len(s)
}

func (s TaskInfosByCreatedAt) Less(i, j int) bool {
	return s[i].CreatedAt < s[j].CreatedAt
}

func (s TaskInfosByCreatedAt) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
