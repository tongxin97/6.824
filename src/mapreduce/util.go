package mapreduce

// ErrCheck is a simple error panic function
func ErrCheck(e error) {
	if e != nil {
		panic(e)
	}
}
