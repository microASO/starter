package getter

// GetFiles ...
func GetFiles(ch chan []byte, i []byte) {
	ch <- i
}

// ProvidePublication ...
func ProvidePublication(test []byte) ([]byte, error) {
	return test, nil
}
