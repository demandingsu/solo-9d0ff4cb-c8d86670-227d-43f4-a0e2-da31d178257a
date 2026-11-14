"""LineDecoder：半包、粘包、CRLF、断连清缓冲。"""
from app.line_buffer import LineDecoder


def test_split_across_chunks():
    dec = LineDecoder()
    assert dec.feed(b'{"weight_g": 0.0') == []
    assert dec.pending() > 0
    assert dec.feed(b', "stable": true}\n') == ['{"weight_g": 0.0, "stable": true}']
    assert dec.pending() == 0


def test_multiple_lines_in_one_chunk():
    dec = LineDecoder()
    chunk = b'{"a":1}\n{"b":2}\n{"c":3}\n'
    assert dec.feed(chunk) == ['{"a":1}', '{"b":2}', '{"c":3}']


def test_crlf_and_byte_by_byte():
    dec = LineDecoder()
    payload = '{"weight_g": 1.5, "stable": true}\r\n'.encode()
    out = []
    for byte in payload:  # 极端拆包：逐字节到达
        out.extend(dec.feed(bytes([byte])))
    assert out == ['{"weight_g": 1.5, "stable": true}']


def test_reset_drops_half_packet():
    dec = LineDecoder()
    dec.feed(b'{"half":')
    assert dec.pending() == 8
    dec.reset()
    assert dec.pending() == 0
    assert dec.feed(b'{"full": true}\n') == ['{"full": true}']


def test_empty_chunks_and_blank_lines():
    dec = LineDecoder()
    assert dec.feed(b"") == []
    assert dec.feed(b"\n") == [""]
    assert dec.feed(b"  \n") == ["  "]
