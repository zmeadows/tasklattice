from __future__ import annotations

from tasklattice.source import Source

def test_pos_to_line_col_basic():
    s = Source(None, "ab\nc\r\nd\n")
    # indexes: 0 1 2 3 4 5 6 7  (len=8)
    assert s.pos_to_line_col(0) == (1, 1)
    assert s.pos_to_line_col(2) == (1, 3)     # '\n' at end of line 1
    assert s.pos_to_line_col(3) == (2, 1)     # 'c'
    assert s.pos_to_line_col(5) == (2, 3)     # end of CRLF line
    assert s.pos_to_line_col(8) == (4, 1)     # caret at EOF (line 4 start)

# [ FUTURE TESTS ]
# * Single-line, multi-line, trailing newline, CRLF ("\r\n") cases
# * pos=0, pos at each line start, last character, and pos=len(contents)
# * Spans at boundaries; out-of-bounds raises
# * Empty file behavior (whatever you choose)
