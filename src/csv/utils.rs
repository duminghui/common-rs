use super::parser::next_line_position;

pub(crate) fn get_file_chunks(
    bytes: &[u8],
    n_chunks: usize,
    expected_fields: Option<usize>,
    separator: u8,
    quote_char: Option<u8>,
    eol_char: u8,
) -> Vec<(usize, usize)> {
    let mut last_pos = 0;
    let total_len = bytes.len();
    let chunk_size = total_len / n_chunks;
    let mut offsets = Vec::with_capacity(n_chunks);
    for _ in 0..n_chunks {
        let search_pos = last_pos + chunk_size;

        if search_pos >= bytes.len() {
            break;
        }

        let end_pos = match next_line_position(
            &bytes[search_pos..],
            expected_fields,
            separator,
            quote_char,
            eol_char,
        ) {
            Some(pos) => search_pos + pos,
            None => {
                break;
            },
        };
        offsets.push((last_pos, end_pos));
        last_pos = end_pos;
    }
    offsets.push((last_pos, total_len));
    offsets
}

// Faster than collecting from a flattened iterator.
pub fn flatten<T: Clone, R: AsRef<[T]>>(bufs: &[R]) -> Vec<T> {
    let len = bufs.iter().map(|b| b.as_ref().len()).sum();

    let mut out = Vec::with_capacity(len);
    for b in bufs {
        out.extend_from_slice(b.as_ref());
    }
    out
}
