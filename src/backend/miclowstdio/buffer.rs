#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TopicInputBuffer {
    active_topic: Option<String>,
    buffered_lines: Vec<String>,
}

#[derive(Debug, Default)]
pub struct InputBufferManager {
    buffers: std::collections::HashMap<String, TopicInputBuffer>,
}

impl TopicInputBuffer {
    pub fn new() -> Self {
        Self {
            active_topic: None,
            buffered_lines: Vec::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        self.active_topic.is_some()
    }

    pub fn start(&mut self, topic: String) {
        self.active_topic = Some(topic);
        self.buffered_lines.clear();
    }

    pub fn push_line(&mut self, line: String) {
        self.buffered_lines.push(line);
    }

    pub fn try_finish(&mut self, closing_topic: &str) -> Option<(String, String)> {
        match &self.active_topic {
            Some(current_topic) if current_topic == closing_topic => {
                let topic = current_topic.clone();
                let data = self.buffered_lines.concat();
                self.active_topic = None;
                self.buffered_lines.clear();
                Some((topic, data))
            }
            _ => None,
        }
    }

    pub fn flush_unfinished(&mut self) -> Option<(String, String)> {
        match &self.active_topic {
            Some(current_topic) => {
                let topic = current_topic.clone();
                let data = self.buffered_lines.concat();
                self.active_topic = None;
                self.buffered_lines.clear();
                Some((topic, data))
            }
            None => None,
        }
    }
}

impl InputBufferManager {
    pub fn new() -> Self {
        Self {
            buffers: std::collections::HashMap::new(),
        }
    }

    pub fn get_or_create_buffer(&mut self, task_id: &str) -> &mut TopicInputBuffer {
        self.buffers
            .entry(task_id.to_string())
            .or_insert_with(TopicInputBuffer::new)
    }

    pub fn consume_stream_line(
        &mut self,
        task_id: &str,
        line: &str,
    ) -> Result<StreamOutcome, String> {
        let buffer = self.get_or_create_buffer(task_id);
        consume_stream_line(buffer, line)
    }

    pub fn flush_all_unfinished(&mut self) -> Vec<(String, String, String)> {
        let mut results = Vec::new();
        for (task_id, buffer) in self.buffers.iter_mut() {
            if let Some((topic, data)) = buffer.flush_unfinished() {
                results.push((task_id.clone(), topic, data));
            }
        }
        results
    }

    pub fn remove_task(&mut self, task_id: &str) -> Option<TopicInputBuffer> {
        self.buffers.remove(task_id)
    }

    pub fn has_active_buffers(&self) -> bool {
        self.buffers.values().any(|buffer| buffer.is_active())
    }
}

fn view_without_crlf(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.ends_with(b"\r\n") {
        &s[..s.len() - 2]
    } else if bytes.ends_with(b"\n") || bytes.ends_with(b"\r") {
        &s[..s.len() - 1]
    } else {
        s
    }
}

fn parse_quoted_topic(input: &str) -> Result<(String, usize), String> {
    let mut escaped = false;
    let mut topic = String::new();

    // 文字境界を追跡しながら処理
    for (i, c) in input.char_indices().skip(1) {
        // 最初の"をスキップ
        if escaped {
            topic.push(c);
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c == '"' {
            if topic.is_empty() {
                return Err("empty topic is not allowed".to_string());
            }
            return Ok((topic, i + c.len_utf8())); // バイトインデックスを返す
        }
        topic.push(c);
    }
    Err("unterminated quoted topic".to_string())
}

pub enum LineParseResult {
    SingleLine { topic: String, value: String },
    MultilineStart { topic: String },
    MultilineEnd { topic: String },
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
        // parse_quoted_topicが失敗した場合はNotSpecialを返す（エラーにしない）
        match parse_quoted_topic(input) {
            Ok((topic, idx_after_topic)) => {
                let rest = &input[idx_after_topic..];
                // 複数行開始の判定: "key":: の完全一致のみ
                if rest == "::" {
                    return Ok(LineParseResult::MultilineStart { topic });
                }
                // 1行形式の判定: "key": data 形式
                let rest_trim_left = rest.trim_start();
                if rest_trim_left.starts_with(':') {
                    let after_colon = &rest_trim_left[1..];
                    let value = after_colon.trim();
                    return Ok(LineParseResult::SingleLine {
                        topic,
                        value: value.to_string(),
                    });
                }
            }
            Err(_) => {
                // parse_quoted_topicが失敗した場合はNotSpecialとして扱う
                return Ok(LineParseResult::NotSpecial);
            }
        }
        return Ok(LineParseResult::NotSpecial);
    }

    // アクティブ状態中の終了判定: ::"key" の完全一致のみ
    if input.starts_with("::\"") {
        let after = &input["::".len()..];
        match parse_quoted_topic(after) {
            Ok((topic, idx)) => {
                if idx == after.len() {
                    return Ok(LineParseResult::MultilineEnd { topic });
                }
            }
            Err(_) => {
                // パース失敗はNotSpecial
            }
        }
    }
    if input.starts_with("::") {
        let after = &input["::".len()..];
        if after.starts_with(' ') || after.starts_with('\t') {
            return Ok(LineParseResult::NotSpecial);
        }
    }

    Ok(LineParseResult::NotSpecial)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamOutcome {
    Emit { topic: String, data: String },
    Plain(String),
    None,
}

pub fn strip_crlf(s: &str) -> &str {
    view_without_crlf(s)
}

pub fn consume_stream_line(
    buffer: &mut TopicInputBuffer,
    line: &str,
) -> Result<StreamOutcome, String> {
    if buffer.is_active() {
        // アクティブ状態中は終了文字（::"key"）以外は通常のデータとしてバッファに追加
        match parse_line(line)? {
            LineParseResult::MultilineEnd { topic } => {
                if let Some((active_topic, data)) = buffer.try_finish(&topic) {
                    Ok(StreamOutcome::Emit {
                        topic: active_topic,
                        data,
                    })
                } else {
                    // トピック名が一致しない場合は通常のデータとしてバッファに追加
                    buffer.push_line(line.to_string());
                    Ok(StreamOutcome::None)
                }
            }
            _ => {
                // 終了文字以外は全て通常のデータとしてバッファに追加
                buffer.push_line(line.to_string());
                Ok(StreamOutcome::None)
            }
        }
    } else {
        match parse_line(line)? {
            LineParseResult::SingleLine { topic, value } => {
                Ok(StreamOutcome::Emit { topic, data: value })
            }
            LineParseResult::MultilineStart { topic } => {
                buffer.start(topic);
                Ok(StreamOutcome::None)
            }
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
    use super::{consume_stream_line, strip_crlf, InputBufferManager, StreamOutcome};

    #[test]
    fn single_line_parsing() {
        let buf = TopicInputBuffer::new();
        match super::parse_line("\"msg\"  :   hello") {
            Ok(super::LineParseResult::SingleLine { topic, value }) => {
                assert_eq!(topic, "msg");
                assert_eq!(value, "hello");
            }
            _ => panic!("expected single line"),
        }
        assert!(!buf.is_active());
    }

    #[test]
    fn multiline_basic() {
        let mut buf = TopicInputBuffer::new();
        match super::parse_line("\"note\"::") {
            Ok(super::LineParseResult::MultilineStart { topic }) => assert_eq!(topic, "note"),
            _ => panic!("start"),
        }
        buf.start("note".to_string());
        assert!(buf.is_active());
        buf.push_line("line1\n".to_string());
        buf.push_line("line2".to_string());
        match super::parse_line("::\"note\"") {
            Ok(super::LineParseResult::MultilineEnd { topic }) => assert_eq!(topic, "note"),
            _ => panic!("end"),
        }
        let res = buf.try_finish("note");
        assert_eq!(res, Some(("note".to_string(), "line1\nline2".to_string())));
        assert!(!buf.is_active());
    }

    #[test]
    fn nested_marker_should_not_start_new_buffer() {
        let mut buf = TopicInputBuffer::new();
        match super::parse_line("\"key\"::") {
            Ok(super::LineParseResult::MultilineStart { topic }) => assert_eq!(topic, "key"),
            _ => panic!("start"),
        }
        buf.start("key".to_string());
        assert!(buf.is_active());
        buf.push_line("\"key2\"::\n".to_string());
        buf.push_line("value\n".to_string());
        buf.push_line("::\"key2\"\n".to_string());
        match super::parse_line("::\"key\"") {
            Ok(super::LineParseResult::MultilineEnd { topic }) => assert_eq!(topic, "key"),
            _ => panic!("end"),
        }
        let res = buf.try_finish("key");
        assert_eq!(
            res,
            Some((
                "key".to_string(),
                "\"key2\"::\nvalue\n::\"key2\"\n".to_string()
            ))
        );
    }

    #[test]
    fn spaces_rules_and_empty_value() {
        match super::parse_line("\"k\" :    ") {
            Ok(super::LineParseResult::SingleLine { topic, value }) => {
                assert_eq!(topic, "k");
                assert_eq!(value, "");
            }
            _ => panic!("single empty value"),
        }
        match super::parse_line("\" k \": v") {
            Ok(super::LineParseResult::SingleLine { topic, value }) => {
                assert_eq!(topic, " k ");
                assert_eq!(value, "v");
            }
            _ => panic!("topic with surrounding spaces should be allowed"),
        }
        assert!(matches!(
            super::parse_line(" \"k\"::"),
            Ok(super::LineParseResult::NotSpecial)
        ));
        assert!(matches!(
            super::parse_line(":: \"k\""),
            Ok(super::LineParseResult::NotSpecial)
        ));
    }

    #[test]
    fn escaped_quotes_in_key() {
        match super::parse_line("\"ke\\\"y\": v") {
            Ok(super::LineParseResult::SingleLine { topic, value }) => {
                assert_eq!(topic, "ke\"y");
                assert_eq!(value, "v");
            }
            _ => panic!("escaped topic"),
        }
    }

    #[test]
    fn multiline_no_normalization_and_trailing_empty_line() {
        let mut buf = TopicInputBuffer::new();
        match super::parse_line("\"n\"::") {
            Ok(super::LineParseResult::MultilineStart { topic }) => assert_eq!(topic, "n"),
            _ => panic!(),
        }
        buf.start("n".to_string());
        buf.push_line("line1\r\n".to_string());
        buf.push_line("\r\n".to_string());
        match super::parse_line("::\"n\"") {
            Ok(super::LineParseResult::MultilineEnd { topic }) => assert_eq!(topic, "n"),
            _ => panic!(),
        }
        let res = buf.try_finish("n").unwrap();
        assert_eq!(res.1, "line1\r\n\r\n");
    }

    #[test]
    fn quoted_value_is_preserved() {
        match super::parse_line("\"k\": \" v with space \"") {
            Ok(super::LineParseResult::SingleLine { topic, value }) => {
                assert_eq!(topic, "k");
                assert_eq!(value, "\" v with space \"");
            }
            _ => panic!("quoted value"),
        }
    }

    #[test]
    fn leading_whitespace_is_error_for_special_lines() {
        assert!(matches!(
            super::parse_line("  \"k\": v"),
            Ok(super::LineParseResult::NotSpecial)
        ));
        assert!(matches!(
            super::parse_line("\t\"k\"::"),
            Ok(super::LineParseResult::NotSpecial)
        ));
        assert!(matches!(
            super::parse_line("  ::\"k\""),
            Ok(super::LineParseResult::NotSpecial)
        ));
    }

    #[test]
    fn multiline_end_with_extra_chars_is_error() {
        assert!(matches!(
            super::parse_line("::\"k\" extra"),
            Ok(super::LineParseResult::NotSpecial)
        ));
    }

    #[test]
    fn unexpected_end_emits_error_path_assumed_at_call_site() {
        assert!(matches!(
            super::parse_line("::\"k\""),
            Ok(super::LineParseResult::MultilineEnd { .. }) | Err(_)
        ));
    }

    #[test]
    fn flush_unfinished_emits_buffered_raw_text() {
        let mut buf = TopicInputBuffer::new();
        buf.start("note".to_string());
        buf.push_line("line A\n".to_string());
        buf.push_line("line B".to_string());
        let flushed = buf.flush_unfinished();
        assert_eq!(
            flushed,
            Some(("note".to_string(), "line A\nline B".to_string()))
        );
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
            StreamOutcome::Emit { topic, data } => {
                assert_eq!(topic, "k");
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
            StreamOutcome::Emit { topic, data } => {
                assert_eq!(topic, "note");
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
            StreamOutcome::Emit { topic, data } => {
                assert_eq!(topic, "a");
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
    fn consume_stream_line_error_on_unterminated_topic() {
        let mut buf = TopicInputBuffer::new();
        // 未終了のトピックはエラーではなくNotSpecialとして扱われる
        let result = consume_stream_line(&mut buf, "\"unterminated").unwrap();
        assert!(matches!(result, StreamOutcome::Plain(_)));
        assert!(!buf.is_active());
    }

    #[test]
    fn input_buffer_manager_basic_operations() {
        let mut manager = InputBufferManager::new();

        // 新しいタスクのバッファを作成
        let buffer = manager.get_or_create_buffer("task1");
        assert!(!buffer.is_active());

        // 複数行の処理
        let result = manager.consume_stream_line("task1", "\"msg\"::").unwrap();
        assert!(matches!(result, StreamOutcome::None));

        let result = manager.consume_stream_line("task1", "line1\n").unwrap();
        assert!(matches!(result, StreamOutcome::None));

        let result = manager.consume_stream_line("task1", "line2").unwrap();
        assert!(matches!(result, StreamOutcome::None));

        let result = manager.consume_stream_line("task1", "::\"msg\"").unwrap();
        match result {
            StreamOutcome::Emit { topic, data } => {
                assert_eq!(topic, "msg");
                assert_eq!(data, "line1\nline2");
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
    }

    #[test]
    fn input_buffer_manager_multiple_tasks() {
        let mut manager = InputBufferManager::new();

        // タスク1の複数行処理
        let _ = manager
            .consume_stream_line("task1", "\"stdout\"::")
            .unwrap();
        let _ = manager.consume_stream_line("task1", "task1_data").unwrap();

        // タスク2の複数行処理（同時実行）
        let _ = manager
            .consume_stream_line("task2", "\"stdout\"::")
            .unwrap();
        let _ = manager.consume_stream_line("task2", "task2_data").unwrap();

        // タスク1を完了
        let result1 = manager
            .consume_stream_line("task1", "::\"stdout\"")
            .unwrap();
        match result1 {
            StreamOutcome::Emit { topic, data } => {
                assert_eq!(topic, "stdout");
                assert_eq!(data, "task1_data");
            }
            other => panic!("unexpected outcome: {:?}", other),
        }

        // タスク2を完了
        let result2 = manager
            .consume_stream_line("task2", "::\"stdout\"")
            .unwrap();
        match result2 {
            StreamOutcome::Emit { topic, data } => {
                assert_eq!(topic, "stdout");
                assert_eq!(data, "task2_data");
            }
            other => panic!("unexpected outcome: {:?}", other),
        }
    }

    #[test]
    fn input_buffer_manager_flush_unfinished() {
        let mut manager = InputBufferManager::new();

        // 未完了のバッファを作成
        let _ = manager.consume_stream_line("task1", "\"msg\"::").unwrap();
        let _ = manager
            .consume_stream_line("task1", "unfinished_data")
            .unwrap();

        // フラッシュ
        let unfinished = manager.flush_all_unfinished();
        assert_eq!(unfinished.len(), 1);
        assert_eq!(unfinished[0].0, "task1");
        assert_eq!(unfinished[0].1, "msg");
        assert_eq!(unfinished[0].2, "unfinished_data");
    }

    #[test]
    fn input_buffer_manager_remove_task() {
        let mut manager = InputBufferManager::new();

        // タスクを作成
        let _ = manager.consume_stream_line("task1", "\"msg\"::").unwrap();
        let _ = manager.consume_stream_line("task1", "data").unwrap();

        // タスクを削除
        let removed = manager.remove_task("task1");
        assert!(removed.is_some());

        // 削除されたタスクのバッファは存在しない
        let buffer = manager.get_or_create_buffer("task1");
        assert!(!buffer.is_active());
    }
}
