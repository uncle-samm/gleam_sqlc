use std::fmt::Write;

pub struct CodeWriter {
    buf: String,
    indent: usize,
}

impl CodeWriter {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            indent: 0,
        }
    }

    pub fn line(&mut self, text: &str) {
        if text.is_empty() {
            self.buf.push('\n');
        } else {
            for _ in 0..self.indent {
                self.buf.push_str("  ");
            }
            self.buf.push_str(text);
            self.buf.push('\n');
        }
    }

    pub fn writef(&mut self, args: std::fmt::Arguments<'_>) {
        for _ in 0..self.indent {
            self.buf.push_str("  ");
        }
        self.buf.write_fmt(args).unwrap();
        self.buf.push('\n');
    }

    pub fn indent(&mut self) {
        self.indent += 1;
    }

    pub fn dedent(&mut self) {
        self.indent = self.indent.saturating_sub(1);
    }

    pub fn blank(&mut self) {
        self.buf.push('\n');
    }

    pub fn into_string(self) -> String {
        self.buf
    }
}
