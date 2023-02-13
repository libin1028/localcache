package localcache

type HashFunc interface {
	Sum64(string) uint64
}

func NewDefaultHashFunc() HashFunc {
	return fnv64a{}
}
