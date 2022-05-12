package utils

import "strconv"

func IntCommaB(num int) string {
	str := strconv.Itoa(num)
	l_str := len(str)
	digits := l_str
	if num < 0 {
		digits--
	}
	commas := (digits+2)/3 - 1
	l_buf := l_str + commas
	var sbuf [32]byte // pre allocate buffer at stack rather than make([]byte,n)
	buf := sbuf[0:l_buf]
	// copy str from the end
	for s_i, b_i, c3 := l_str-1, l_buf-1, 0; ; {
		buf[b_i] = str[s_i]
		if s_i == 0 {
			return string(buf)
		}
		s_i--
		b_i--
		// insert comma every 3 chars
		c3++
		if c3 == 3 && (s_i > 0 || num > 0) {
			buf[b_i] = ','
			b_i--
			c3 = 0
		}
	}
}
