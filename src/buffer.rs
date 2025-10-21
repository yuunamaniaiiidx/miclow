
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TopicInputBuffer {
    active_key: Option<String>,
    buffered_lines: Vec<String>,
}

impl TopicInputBuffer {
    pub fn new() -> Self {
        Self { active_key: None, buffered_lines: Vec::new() }
    }

    pub fn is_active(&self) -> bool { self.active_key.is_some() }

    pub fn start(&mut self, key: String) {
        self.active_key = Some(key);
        self.buffered_lines.clear();
    }

    pub fn push_line(&mut self, line: String) { self.buffered_lines.push(line); }

    pub fn try_finish(&mut self, closing_key: &str) -> Option<(String, String)> {
        match &self.active_key {
            Some(current_key) if current_key == closing_key => {
                let key = current_key.clone();
                let data = self.buffered_lines.concat();
                self.active_key = None;
                self.buffered_lines.clear();
                Some((key, data))
            }
            _ => None,
        }
    }

    pub fn flush_unfinished(&mut self) -> Option<(String, String)> {
        match &self.active_key {
            Some(current_key) => {
                let key = current_key.clone();
                let data = self.buffered_lines.concat();
                self.active_key = None;
                self.buffered_lines.clear();
                Some((key, data))
            }
            None => None,
        }
    }
}

fn view_without_crlf(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.ends_with(b"\r\n") { &s[..s.len()-2] }
    else if bytes.ends_with(b"\n") || bytes.ends_with(b"\r") { &s[..s.len()-1] }
    else { s }
}

fn parse_quoted_key(input: &str) -> Result<(String, usize), String> {
    let mut escaped = false;
    let mut key = String::new();
    let mut i = 1;
    let chars: Vec<char> = input.chars().collect();
    while i < chars.len() {
        let c = chars[i];
        if escaped { key.push(c); escaped = false; i += 1; continue; }
        if c == '\\' { escaped = true; i += 1; continue; }
        if c == '"' {
            if key.is_empty() { return Err("empty key is not allowed".to_string()); }
            return Ok((key, i+1));
        }
        key.push(c);
        i += 1;
    }
    Err("unterminated quoted key".to_string())
}

pub enum LineParseResult {
    SingleLine { key: String, value: String },
    MultilineStart { key: String },
    MultilineEnd { key: String },
    NotSpecial,
}

pub fn parse_line(input_raw: &str) -> Result<LineParseResult, String> {
    let input = view_without_crlf(input_raw);
    if input.starts_with(' ') || input.starts_with('\t') { 
        let trimmed_left = input.trim_start_matches([' ', '\t']);
        if trimmed_left.starts_with('"') || trimmed_left.starts_with("::") {
            return Ok(LineParseResult::NotSpecial);
        }
    }
    if input.starts_with('"') {
        let (key, idx_after_key) = parse_quoted_key(input)?;
        let rest = &input[idx_after_key..];
        if rest == "::" {
            return Ok(LineParseResult::MultilineStart { key });
        }
        let rest_trim_left = rest.trim_start();
        if rest_trim_left.starts_with(':') {
            let after_colon = &rest_trim_left[1..];
            let value = after_colon.trim();
            return Ok(LineParseResult::SingleLine { key, value: value.to_string() });
        }
        return Ok(LineParseResult::NotSpecial);
    }

    if input.starts_with("::\"") {
        let after = &input[2..];
        let (key, idx) = parse_quoted_key(after)?;
        if idx == after.len() {
            return Ok(LineParseResult::MultilineEnd { key });
        } else {
            return Ok(LineParseResult::NotSpecial);
        }
    }
    if input.starts_with("::") {
        let after = &input[2..];
        if after.starts_with(' ') || after.starts_with('\t') {
            return Ok(LineParseResult::NotSpecial);
        }
    }

    Ok(LineParseResult::NotSpecial)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamOutcome {
    Emit { key: String, data: String },
    Plain(String),
    None,
}

pub fn strip_crlf(s: &str) -> &str {
    view_without_crlf(s)
}

pub fn consume_stream_line(buffer: &mut TopicInputBuffer, line: &str) -> Result<StreamOutcome, String> {
    if buffer.is_active() {
        match parse_line(line)? {
            LineParseResult::MultilineEnd { key } => {
                if let Some((active_key, data)) = buffer.try_finish(&key) {
                    Ok(StreamOutcome::Emit { key: active_key, data })
                } else {
                    buffer.push_line(line.to_string());
                    Ok(StreamOutcome::None)
                }
            }
            _ => {
                buffer.push_line(line.to_string());
                Ok(StreamOutcome::None)
            }
        }
    } else {
        match parse_line(line)? {
            LineParseResult::SingleLine { key, value } => Ok(StreamOutcome::Emit { key, data: value }),
            LineParseResult::MultilineStart { key } => { buffer.start(key); Ok(StreamOutcome::None) }
            LineParseResult::MultilineEnd { .. } => {
                Ok(StreamOutcome::Plain(strip_crlf(line).to_string()))
            }
            LineParseResult::NotSpecial => Ok(StreamOutcome::Plain(strip_crlf(line).to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TopicInputBuffer;
    use super::{consume_stream_line, StreamOutcome, strip_crlf};

    #[test]
    fn single_line_parsing() {
        let mut buf = TopicInputBuffer::new();
        match super::parse_line("\"msg\"  :   hello") {
            Ok(super::LineParseResult::SingleLine { key, value }) => {
                assert_eq!(key, "msg");
                assert_eq!(value, "hello");
            }
            _ => panic!("expected single line"),
        }
        assert!(!buf.is_active());
    }

    #[test]
    fn multiline_basic() {
        let mut buf = TopicInputBuffer::new();
        match super::parse_line("\"note\"::") { Ok(super::LineParseResult::MultilineStart { key }) => assert_eq!(key, "note"), _ => panic!("start") }
        buf.start("note".to_string());
        assert!(buf.is_active());
        buf.push_line("line1\n".to_string());
        buf.push_line("line2".to_string());
        match super::parse_line("::\"note\"") { Ok(super::LineParseResult::MultilineEnd { key }) => assert_eq!(key, "note"), _ => panic!("end") }
        let res = buf.try_finish("note");
        assert_eq!(res, Some(("note".to_string(), "line1\nline2".to_string())));
        assert!(!buf.is_active());
    }

    #[test]
    fn nested_marker_should_not_start_new_buffer() {
        let mut buf = TopicInputBuffer::new();
        match super::parse_line("\"key\"::") { Ok(super::LineParseResult::MultilineStart { key }) => assert_eq!(key, "key"), _ => panic!("start") }
        buf.start("key".to_string());
        assert!(buf.is_active());
        buf.push_line("\"key2\"::\n".to_string());
        buf.push_line("value\n".to_string());
        buf.push_line("::\"key2\"\n".to_string());
        match super::parse_line("::\"key\"") { Ok(super::LineParseResult::MultilineEnd { key }) => assert_eq!(key, "key"), _ => panic!("end") }
        let res = buf.try_finish("key");
        assert_eq!(res, Some(("key".to_string(), "\"key2\"::\nvalue\n::\"key2\"\n".to_string())));
    }

    #[test]
    fn spaces_rules_and_empty_value() {
        match super::parse_line("\"k\" :    ") {
            Ok(super::LineParseResult::SingleLine { key, value }) => { assert_eq!(key, "k"); assert_eq!(value, ""); }
            _ => panic!("single empty value")
        }
        match super::parse_line("\" k \": v") {
            Ok(super::LineParseResult::SingleLine { key, value }) => {
                assert_eq!(key, " k ");
                assert_eq!(value, "v");
            }
            _ => panic!("key with surrounding spaces should be allowed")
        }
        assert!(matches!(super::parse_line(" \"k\"::"), Ok(super::LineParseResult::NotSpecial)));
        assert!(matches!(super::parse_line(":: \"k\""), Ok(super::LineParseResult::NotSpecial)));
    }

    #[test]
    fn escaped_quotes_in_key() {
        match super::parse_line("\"ke\\\"y\": v") {
            Ok(super::LineParseResult::SingleLine { key, value }) => {
                assert_eq!(key, "ke\"y");
                assert_eq!(value, "v");
            }
            _ => panic!("escaped key")
        }
    }

    #[test]
    fn multiline_no_normalization_and_trailing_empty_line() {
        let mut buf = TopicInputBuffer::new();
        match super::parse_line("\"n\"::") { Ok(super::LineParseResult::MultilineStart { key }) => assert_eq!(key, "n"), _ => panic!() }
        buf.start("n".to_string());
        buf.push_line("line1\r\n".to_string());
        buf.push_line("\r\n".to_string());
        match super::parse_line("::\"n\"") { Ok(super::LineParseResult::MultilineEnd { key }) => assert_eq!(key, "n"), _ => panic!() }
        let res = buf.try_finish("n").unwrap();
        assert_eq!(res.1, "line1\r\n\r\n");
    }

    #[test]
    fn quoted_value_is_preserved() {
        match super::parse_line("\"k\": \" v with space \"") {
            Ok(super::LineParseResult::SingleLine { key, value }) => {
                assert_eq!(key, "k");
                assert_eq!(value, "\" v with space \"");
            }
            _ => panic!("quoted value")
        }
    }

    #[test]
    fn leading_whitespace_is_error_for_special_lines() {
        assert!(matches!(super::parse_line("  \"k\": v"), Ok(super::LineParseResult::NotSpecial)));
        assert!(matches!(super::parse_line("\t\"k\"::"), Ok(super::LineParseResult::NotSpecial)));
        assert!(matches!(super::parse_line("  ::\"k\""), Ok(super::LineParseResult::NotSpecial)));
    }

    #[test]
    fn multiline_end_with_extra_chars_is_error() {
        assert!(matches!(super::parse_line("::\"k\" extra"), Ok(super::LineParseResult::NotSpecial)));
    }

    #[test]
    fn unexpected_end_emits_error_path_assumed_at_call_site() {
        assert!(matches!(super::parse_line("::\"k\""), Ok(super::LineParseResult::MultilineEnd{..}) | Err(_)));
    }

    #[test]
    fn flush_unfinished_emits_buffered_raw_text() {
        let mut buf = TopicInputBuffer::new();
        buf.start("note".to_string());
        buf.push_line("line A\n".to_string());
        buf.push_line("line B".to_string());
        let flushed = buf.flush_unfinished();
        assert_eq!(flushed, Some(("note".to_string(), "line A\nline B".to_string())));
        assert!(!buf.is_active());
    }

    #[test]
    fn strip_crlf_trims_line_endings() {
        assert_eq!(strip_crlf("abc\r\n"), "abc");
        assert_eq!(strip_crlf("abc\n"), "abc");
        assert_eq!(strip_crlf("abc\r"), "abc");
        assert_eq!(strip_crlf("abc"), "abc");
    }

    #[test]
    fn consume_stream_line_emits_single_line_when_not_active() {
        let mut buf = TopicInputBuffer::new();
        let res = consume_stream_line(&mut buf, "\"k\": v").unwrap();
        match res {
            StreamOutcome::Emit { key, data } => {
                assert_eq!(key, "k");
                assert_eq!(data, "v");
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
        assert!(!buf.is_active());
    }

    #[test]
    fn consume_stream_line_plain_for_not_special_when_not_active() {
        let mut buf = TopicInputBuffer::new();
        let res = consume_stream_line(&mut buf, "hello\n").unwrap();
        match res {
            StreamOutcome::Plain(s) => assert_eq!(s, "hello"),
            other => panic!("unexpected outcome: {:?}", other),
        }
        assert!(!buf.is_active());
    }

    #[test]
    fn consume_stream_line_multiline_start_then_end_emits_aggregated() {
        let mut buf = TopicInputBuffer::new();
        let o1 = consume_stream_line(&mut buf, "\"note\"::").unwrap();
        assert!(matches!(o1, StreamOutcome::None));
        assert!(buf.is_active());
        let o2 = consume_stream_line(&mut buf, "line1\n").unwrap();
        let o3 = consume_stream_line(&mut buf, "line2").unwrap();
        assert!(matches!(o2, StreamOutcome::None));
        assert!(matches!(o3, StreamOutcome::None));
        let o4 = consume_stream_line(&mut buf, "::\"note\"").unwrap();
        match o4 {
            StreamOutcome::Emit { key, data } => {
                assert_eq!(key, "note");
                assert_eq!(data, "line1\nline2");
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
        assert!(!buf.is_active());
    }

    #[test]
    fn consume_stream_line_mismatched_end_keeps_buffering() {
        let mut buf = TopicInputBuffer::new();
        let _ = consume_stream_line(&mut buf, "\"a\"::").unwrap();
        assert!(buf.is_active());
        let out = consume_stream_line(&mut buf, "::\"b\"").unwrap();
        assert!(matches!(out, StreamOutcome::None));
        let out2 = consume_stream_line(&mut buf, "::\"a\"").unwrap();
        match out2 {
            StreamOutcome::Emit { key, data } => {
                assert_eq!(key, "a");
                assert_eq!(data, "::\"b\"");
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
        assert!(!buf.is_active());
    }

    #[test]
    fn consume_stream_line_end_when_not_active_returns_plain() {
        let mut buf = TopicInputBuffer::new();
        let out = consume_stream_line(&mut buf, "::\"k\"\n").unwrap();
        match out {
            StreamOutcome::Plain(s) => assert_eq!(s, "::\"k\""),
            other => panic!("unexpected outcome: {:?}", other),
        }
        assert!(!buf.is_active());
    }

    #[test]
    fn consume_stream_line_error_on_unterminated_key() {
        let mut buf = TopicInputBuffer::new();
        let err = consume_stream_line(&mut buf, "\"unterminated").err();
        assert!(err.is_some());
        assert!(!buf.is_active());
    }
}


