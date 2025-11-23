use crate::background_worker_registry::{
    BackgroundWorker, BackgroundWorkerContext, WorkerReadiness,
};
use async_trait::async_trait;
use console::{style, Style, Term};
use log::{Level, LevelFilter, Log, Metadata, Record, SetLoggerError};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Clone, Debug)]
pub struct LogEvent {
    pub level: Level,
    pub target: Arc<str>,
    pub msg: Arc<str>,
    pub pod_id: Option<Arc<str>>,
    pub task_name: Option<Arc<str>>, // 追加
}

pub struct ChannelLogger {
    tx: UnboundedSender<LogEvent>,
}

impl ChannelLogger {
    pub fn new(tx: UnboundedSender<LogEvent>) -> Self {
        Self { tx }
    }
}

impl Log for ChannelLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let _ = self.tx.send(LogEvent {
            level: record.level(),
            target: Arc::from(record.target()),
            msg: Arc::from(format!("{}", record.args())),
            pod_id: None,
            task_name: None, // 追加
        });
    }

    fn flush(&self) {}
}

/// Set ChannelLogger as global logger and max level
pub fn set_channel_logger(
    tx: UnboundedSender<LogEvent>,
    level: LevelFilter,
) -> Result<(), SetLoggerError> {
    log::set_max_level(level);
    log::set_boxed_logger(Box::new(ChannelLogger::new(tx)))
}

/// Determine LevelFilter from RUST_LOG env var (simple parser)
pub fn level_from_env() -> LevelFilter {
    match std::env::var("RUST_LOG").ok().as_deref() {
        Some("trace") => LevelFilter::Trace,
        Some("debug") => LevelFilter::Debug,
        Some("info") => LevelFilter::Info,
        Some("warn") => LevelFilter::Warn,
        Some("error") => LevelFilter::Error,
        Some("") => LevelFilter::Off,
        None => LevelFilter::Off,
        _ => LevelFilter::Off,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserLogKind {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone)]
pub struct UserLogEvent {
    pub pod_id: String,
    pub task_name: Arc<str>,
    pub kind: UserLogKind,
    pub msg: Arc<str>,
}

struct ColorBook {
    map: HashMap<String, Style>,
}

impl ColorBook {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn style_for(&mut self, key: &str) -> Style {
        if let Some(s) = self.map.get(key) {
            return s.clone();
        }
        let (r, g, b) = task_name_to_rgb_256(key);
        let idx = rgb_to_xterm256_index(r, g, b);
        let s = Style::new().color256(idx);
        self.map.insert(key.to_string(), s.clone());
        s
    }
}

// --- Color utilities ---

fn task_name_to_rgb_256(name: &str) -> (u8, u8, u8) {
    let hue = simhash_hue(name) as f32; // 0..360
                                        // Fix saturation/lightness for readability
    let (r, g, b) = hsl_to_rgb(hue, 0.65, 0.55);
    (r, g, b)
}

fn simhash_hue(text: &str) -> u16 {
    let grams = ngrams(text, 3);
    let mut accum = [0i32; 64];
    for g in grams {
        let mut hasher = DefaultHasher::new();
        g.hash(&mut hasher);
        let h = hasher.finish();
        for i in 0..64 {
            if (h >> i) & 1 == 1 {
                accum[i] += 1;
            } else {
                accum[i] -= 1;
            }
        }
    }
    let mut value: u64 = 0;
    for i in 0..64 {
        if accum[i] >= 0 {
            value |= 1u64 << i;
        }
    }
    (value % 360) as u16
}

fn ngrams(s: &str, n: usize) -> Vec<String> {
    let lower = s.to_lowercase();
    let chars: Vec<char> = lower.chars().collect();
    if chars.len() < n {
        return vec![lower];
    }
    let mut out = Vec::with_capacity(chars.len().saturating_sub(n) + 1);
    for i in 0..=chars.len() - n {
        let g: String = chars[i..i + n].iter().collect();
        out.push(g);
    }
    out
}

fn hsl_to_rgb(h_deg: f32, s: f32, l: f32) -> (u8, u8, u8) {
    let h = (h_deg % 360.0) / 60.0; // 0..6
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let x = c * (1.0 - ((h % 2.0) - 1.0).abs());
    let (r1, g1, b1) = if h < 1.0 {
        (c, x, 0.0)
    } else if h < 2.0 {
        (x, c, 0.0)
    } else if h < 3.0 {
        (0.0, c, x)
    } else if h < 4.0 {
        (0.0, x, c)
    } else if h < 5.0 {
        (x, 0.0, c)
    } else {
        (c, 0.0, x)
    };
    let m = l - c / 2.0;
    let r = ((r1 + m) * 255.0).round().clamp(0.0, 255.0) as u8;
    let g = ((g1 + m) * 255.0).round().clamp(0.0, 255.0) as u8;
    let b = ((b1 + m) * 255.0).round().clamp(0.0, 255.0) as u8;
    (r, g, b)
}

fn rgb_to_xterm256_index(r: u8, g: u8, b: u8) -> u8 {
    // Map 0..255 to 0..5
    let to_6 = |v: u8| -> u8 { ((v as f32 / 255.0) * 5.0).round().clamp(0.0, 5.0) as u8 };
    let r6 = to_6(r);
    let g6 = to_6(g);
    let b6 = to_6(b);
    16 + 36 * r6 + 6 * g6 + b6
}

pub struct LogAggregatorWorker {
    rx: UnboundedReceiver<LogEvent>,
}

impl LogAggregatorWorker {
    pub fn new(rx: UnboundedReceiver<LogEvent>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl BackgroundWorker for LogAggregatorWorker {
    fn name(&self) -> &str {
        "log_aggregator"
    }

    fn readiness(&self) -> WorkerReadiness {
        WorkerReadiness::NeedsSignal
    }

    async fn run(self, ctx: BackgroundWorkerContext) {
        let mut rx = self.rx;
        let term = Term::stdout();
        let mut colors = ColorBook::new();
        let mut ready_handle = ctx.ready;
        ready_handle.notify();
        let shutdown = ctx.shutdown;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => { break; }
                maybe = rx.recv() => {
                    if let Some(ev) = maybe {
                        let level_tag = match ev.level {
                            Level::Error => style("ERROR").red().bold().to_string(),
                            Level::Warn => style("WARN ").yellow().to_string(),
                            Level::Info => style("INFO ").cyan().to_string(),
                            Level::Debug => style("DEBUG").blue().to_string(),
                            Level::Trace => style("TRACE").dim().to_string(),
                        };
                        let tag_content = if let Some(ref tname) = ev.task_name {
                            format!("task={}", tname.as_ref())
                        } else {
                            ev.target.as_ref().to_string()
                        };
                        let s = colors.style_for(&tag_content);
                        let tag = s.apply_to(format!("[{}]", tag_content)).to_string();
                        let _ = term.write_line(&format!("{} {} {}", level_tag, tag, ev.msg.as_ref()));
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

pub struct UserLogAggregatorWorker {
    rx: UnboundedReceiver<UserLogEvent>,
}

impl UserLogAggregatorWorker {
    pub fn new(rx: UnboundedReceiver<UserLogEvent>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl BackgroundWorker for UserLogAggregatorWorker {
    fn name(&self) -> &str {
        "user_log_aggregator"
    }

    fn readiness(&self) -> WorkerReadiness {
        WorkerReadiness::NeedsSignal
    }

    async fn run(self, ctx: BackgroundWorkerContext) {
        let mut rx = self.rx;
        let term = Term::stdout();
        let mut colors = ColorBook::new();
        let mut ready_handle = ctx.ready;
        ready_handle.notify();
        let shutdown = ctx.shutdown;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => { break; }
                maybe = rx.recv() => {
                    if let Some(ev) = maybe {
                        let task_style = colors.style_for(ev.task_name.as_ref());
                        let (kind_str, msg_style) = match ev.kind {
                            UserLogKind::Stdout => ("stdout", Style::new().white()),
                            UserLogKind::Stderr => ("stderr", Style::new().red()),
                        };
                        let tag = task_style
                            .apply_to(format!("[{} {}]", ev.task_name.as_ref(), kind_str))
                            .to_string();
                        let _ = term.write_line(&format!(
                            "{} {}",
                            tag,
                            msg_style.apply_to(&ev.msg)
                        ));
                    } else {
                        break;
                    }
                }
            }
        }
    }
}
