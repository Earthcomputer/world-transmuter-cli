use std::fmt::{Display, Formatter};

#[derive(Copy, Clone)]
pub struct Indent {
    count: usize,
}

impl Indent {
    pub fn new() -> Indent {
        Indent { count: 0 }
    }

    pub fn indent(&mut self) {
        self.count += 1;
    }
}

impl Display for Indent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for _ in 0..self.count {
            f.write_str("--")?;
        }
        if self.count > 0 {
            f.write_str(" ")?;
        }
        Ok(())
    }
}
