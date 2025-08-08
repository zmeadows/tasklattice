from tasklattice.param.parser import parse_placeholder
from tasklattice.param.model import Interval
from tasklattice.errors import InvalidPlaceholderError

def test_valid_placeholder() -> None:
    s = '{{TL x = 1.5, type: float, domain: (0, 2], desc: "Gain parameter"}}'
    spec = parse_placeholder(s)
    assert spec.name == "x"
    assert spec.type is float
    assert spec.default == 1.5
    assert isinstance(spec.domain, Interval)
    assert spec.domain.left_closed is False
    assert spec.domain.right_closed is True
    assert spec.desc == "Gain parameter"

def test_invalid_key_typo() -> None:
    s = '{{TL gain = 5, domian: (0, 10]}}'
    try:
        parse_placeholder(s, filename="template.yaml", lineno=42, col=18, source_line=s)
    except InvalidPlaceholderError as e:
        assert "domian" in str(e)
        assert "Did you mean" in str(e)
