pub(crate) struct SplitFields<'a> {
    v:          &'a [u8],
    separator:  u8,
    finished:   bool,
    quote_char: u8,
    quoting:    bool,
    eol_char:   u8,
}

impl<'a> SplitFields<'a> {
    pub(crate) fn new(
        slice: &'a [u8],
        separator: u8,
        quote_char: Option<u8>,
        eol_char: u8,
    ) -> Self {
        Self {
            v: slice,
            separator,
            finished: false,
            quote_char: quote_char.unwrap_or(b'"'),
            quoting: quote_char.is_some(),
            eol_char,
        }
    }

    unsafe fn finish_eol(&mut self, need_escaping: bool, idx: usize) -> Option<(&'a [u8], bool)> {
        self.finished = true;
        debug_assert!(idx <= self.v.len());
        Some((self.v.get_unchecked(..idx), need_escaping))
    }

    fn finish(&mut self, need_escaping: bool) -> Option<(&'a [u8], bool)> {
        self.finished = true;
        Some((self.v, need_escaping))
    }

    fn eof_oel(&self, current_ch: u8) -> bool {
        current_ch == self.separator || current_ch == self.eol_char
    }
}

impl<'a> Iterator for SplitFields<'a> {
    // the bool is used to indicate that it requires escaping
    type Item = (&'a [u8], bool);

    #[inline]
    fn next(&mut self) -> Option<(&'a [u8], bool)> {
        if self.finished {
            return None;
        } else if self.v.is_empty() {
            return self.finish(false);
        }

        let mut needs_escaping = false;
        // There can be strings with separators:
        // "Street, City",

        // SAFETY:
        // we have checked bounds
        let pos = if self.quoting && unsafe { *self.v.get_unchecked(0) } == self.quote_char {
            needs_escaping = true;
            // There can be pair of double-quotes within string.
            // Each of the embedded double-quote characters must be represented
            // by a pair of double-quote characters:
            // e.g. 1997,Ford,E350,"Super, ""luxurious"" truck",20020

            // denotes if we are in a string field, started with a quote
            let mut in_field = false;

            let mut idx = 0u32;
            let mut current_idx = 0u32;
            // micro optimizations
            #[allow(clippy::explicit_counter_loop)]
            for &c in self.v.iter() {
                if c == self.quote_char {
                    // toggle between string field enclosure
                    //      if we encounter a starting '"' -> in_field = true;
                    //      if we encounter a closing '"' -> in_field = false;
                    in_field = !in_field;
                }

                if !in_field && self.eof_oel(c) {
                    if c == self.eol_char {
                        // SAFETY:
                        // we are in bounds
                        return unsafe { self.finish_eol(needs_escaping, current_idx as usize) };
                    }
                    idx = current_idx;
                    break;
                }
                current_idx += 1;
            }

            if idx == 0 {
                return self.finish(needs_escaping);
            }

            idx as usize
        } else {
            match self.v.iter().position(|&c| self.eof_oel(c)) {
                None => return self.finish(needs_escaping),
                Some(idx) => unsafe {
                    // SAFETY:
                    // idx was just found
                    if *self.v.get_unchecked(idx) == self.eol_char {
                        return self.finish_eol(needs_escaping, idx);
                    } else {
                        idx
                    }
                },
            }
        };

        unsafe {
            debug_assert!(pos <= self.v.len());
            // SAFETY:
            // we are in bounds
            let ret = Some((self.v.get_unchecked(..pos), needs_escaping));
            self.v = self.v.get_unchecked(pos + 1..);
            ret
        }
    }
}
