package mindreader

func shortBlockID(in string) string {
	if len(in) > 8 {
		return in[len(in)-8:]
	}
	return in
}
