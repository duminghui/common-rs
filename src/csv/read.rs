use std::fs;
use std::io::Read;
use std::path::Path;

use eyre::OptionExt;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use serde::de::DeserializeOwned;

use super::parser::{
    get_line_stats, is_comment_line, next_line_position, next_line_position_naive, skip_bom,
    skip_line_ending, skip_this_line, skip_whitespace_exclude,
};
use super::utils::{flatten, get_file_chunks};
use crate::csv::POOL;
use crate::AResult;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) enum CommentPrefix {
    /// A single byte character that indicates the start of a comment line.
    Single(u8),
    /// A string that indicates the start of a comment line.
    /// This allows for multiple characters to be used as a comment identifier.
    Multi(String),
}

#[allow(unused)]
impl CommentPrefix {
    /// Creates a new `CommentPrefix` for the `Single` variant.
    pub fn new_single(c: u8) -> Self {
        CommentPrefix::Single(c)
    }

    /// Creates a new `CommentPrefix`. If `Multi` variant is used and the string is longer
    /// than 5 characters, it will return `None`.
    pub fn new_multi(s: String) -> Option<Self> {
        if s.len() <= 5 {
            Some(CommentPrefix::Multi(s))
        } else {
            None
        }
    }
}

pub struct CsvReader {
    skip_rows_before_header: usize,
    skip_rows_after_header:  usize,
    /// Stop reading from the csv after this number of rows is reached
    n_rows:                  Option<usize>,
    n_threads:               Option<usize>,
    has_header:              bool,
    separator:               u8,
    sample_size:             usize,
    comment_prefix:          Option<CommentPrefix>,
    quote_char:              Option<u8>,
    eol_char:                u8,
}

impl Default for CsvReader {
    fn default() -> Self {
        Self::new()
    }
}

impl CsvReader {
    pub fn new() -> Self {
        CsvReader {
            skip_rows_before_header: 0,
            skip_rows_after_header:  0,
            n_rows:                  None,
            n_threads:               None,
            has_header:              false,
            separator:               b',',
            sample_size:             1024,
            comment_prefix:          None,
            quote_char:              Some(b'"'),
            eol_char:                b'\n',
        }
    }

    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    fn find_starting_point<'b>(
        &self,
        mut bytes: &'b [u8],
        quote_char: Option<u8>,
        eol_char: u8,
    ) -> AResult<(&'b [u8], Option<usize>)> {
        let starting_point_offset = bytes.as_ptr() as usize;
        // Skip all leading white space and the occasional utf8-bom
        bytes = skip_whitespace_exclude(skip_bom(bytes), self.separator);
        // \n\n can be a empty string row of a single column
        // in other cases we skip it.
        // if self.schema.len() > 1 {
        bytes = skip_line_ending(bytes, eol_char);
        // }

        if self.skip_rows_before_header > 0 {
            for _ in 0..self.skip_rows_before_header {
                let pos = next_line_position_naive(bytes, eol_char)
                    .ok_or_eyre("not enough lines to skip")?;
                bytes = &bytes[pos..];
            }
        }

        // skip lines that are comments
        while is_comment_line(bytes, self.comment_prefix.as_ref()) {
            bytes = skip_this_line(bytes, quote_char, eol_char);
        }
        // skip header row
        if self.has_header {
            bytes = skip_this_line(bytes, quote_char, eol_char);
        }

        // skip 'n' rows following the header
        if self.skip_rows_after_header > 0 {
            for _ in 0..self.skip_rows_after_header {
                let pos = if is_comment_line(bytes, self.comment_prefix.as_ref()) {
                    next_line_position_naive(bytes, eol_char)
                } else {
                    // we don't pass expected fields
                    // as we want to skip all rows
                    // no matter the no. of fields
                    next_line_position(bytes, None, self.separator, self.quote_char, eol_char)
                }
                .ok_or_eyre("not enough lines to skip")?;

                bytes = &bytes[pos..];
            }
        }
        let starting_point_offset = if bytes.is_empty() {
            None
        } else {
            Some(bytes.as_ptr() as usize - starting_point_offset)
        };

        Ok((bytes, starting_point_offset))
    }

    /// Estimates number of rows and optionally ensure we don't read more than `n_rows`
    /// by slicing `bytes` to the upper bound.
    fn estimate_rows_and_set_upper_bound<'b>(
        &self,
        mut bytes: &'b [u8],
        logging: bool,
        set_upper_bound: bool,
    ) -> (&'b [u8], usize, Option<&'b [u8]>) {
        // initial row guess. We use the line statistic to guess the number of rows to allocate
        let mut total_rows = 128;

        // if we set an upper bound on bytes, keep a reference to the bytes beyond the bound
        let mut remaining_bytes = None;

        // if None, there are less then 128 rows in the file and the statistics don't matter that much
        if let Some((mean, std)) = get_line_stats(
            bytes,
            self.sample_size,
            self.eol_char,
            // Some(self.schema.len()),
            None,
            self.separator,
            self.quote_char,
        ) {
            if logging {
                eprintln!("avg line length: {mean}\nstd. dev. line length: {std}");
            }

            // x % upper bound of byte length per line assuming normally distributed
            // this upper bound assumption is not guaranteed to be accurate
            let line_length_upper_bound = mean + 1.1 * std;
            total_rows = (bytes.len() as f32 / (mean - 0.01 * std)) as usize;

            // if we only need to parse n_rows,
            // we first try to use the line statistics to estimate the total bytes we need to process
            if let Some(n_rows) = self.n_rows {
                total_rows = std::cmp::min(n_rows, total_rows);

                // the guessed upper bound of  the no. of bytes in the file
                let n_bytes = (line_length_upper_bound * (n_rows as f32)) as usize;

                if n_bytes < bytes.len() {
                    if let Some(pos) = next_line_position(
                        &bytes[n_bytes..],
                        // Some(self.schema.len()),
                        None,
                        self.separator,
                        self.quote_char,
                        self.eol_char,
                    ) {
                        if set_upper_bound {
                            (bytes, remaining_bytes) =
                                (&bytes[..n_bytes + pos], Some(&bytes[n_bytes + pos..]))
                        }
                    }
                }
            }
            if logging {
                eprintln!("initial row estimate: {total_rows}")
            }
        }
        (bytes, total_rows, remaining_bytes)
    }

    #[allow(clippy::type_complexity)]
    fn determine_file_chunks_and_statistics<'a>(
        &self,
        n_threads: &mut usize,
        bytes: &'a [u8],
        logging: bool,
    ) -> AResult<(Vec<(usize, usize)>, &'a [u8])> {
        // Make the variable mutable so that we can reassign the sliced file to this variable.
        let (bytes, _) = self.find_starting_point(bytes, self.quote_char, self.eol_char)?;

        let (bytes, total_rows, _) = self.estimate_rows_and_set_upper_bound(bytes, logging, true);

        if total_rows == 128 {
            *n_threads = 1;

            if logging {
                eprintln!("file < 128 rows, no statistics determined")
            }
        }

        let n_file_chunks = *n_threads;

        let chunks = get_file_chunks(
            bytes,
            n_file_chunks,
            // Some(self.schema.len()),
            None,
            self.separator,
            self.quote_char,
            self.eol_char,
        );

        if logging {
            eprintln!(
                "no. of chunks: {} processed by: {n_threads} threads.",
                chunks.len()
            );
        }

        Ok((chunks, bytes))
    }

    fn parse_csv<R>(&mut self, bytes: &[u8]) -> AResult<Vec<R>>
    where
        R: DeserializeOwned + Send + Clone,
    {
        let mut n_threads = self.n_threads.unwrap_or_else(|| POOL.current_num_threads());

        let logging = false;
        let (file_chunks, bytes) =
            self.determine_file_chunks_and_statistics(&mut n_threads, bytes, logging)?;

        let ds_vec = POOL.install(|| {
            file_chunks
                .into_par_iter()
                .enumerate()
                .map(|(idx, (bytes_offset_thread, stop_at_nbytes))| {
                    let local_bytes = &bytes[bytes_offset_thread..stop_at_nbytes];
                    let has_header = if idx == 0 { self.has_header } else { false };
                    let mut rdr = csv::ReaderBuilder::new()
                        .has_headers(has_header)
                        .from_reader(local_bytes);
                    rdr.deserialize::<R>().collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()
        })?;

        let d_vec = flatten(&ds_vec);
        Ok(d_vec)
    }

    pub fn read_csv_file<R>(&mut self, path: impl AsRef<Path>) -> AResult<Vec<R>>
    where
        R: DeserializeOwned + Send + Clone,
    {
        let mut file = fs::File::open(path).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        self.parse_csv::<R>(&bytes)
    }

    #[cfg(feature = "csv-zip")]
    pub fn read_zip_file<R>(&mut self, path: impl AsRef<Path>) -> AResult<(Vec<R>, String)>
    where
        R: DeserializeOwned + Send + Clone,
    {
        use std::io::Cursor;

        let file = fs::File::open(path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut zip_file = archive.by_index(0).unwrap();
        let mut buf = Vec::new();
        zip_file.read_to_end(&mut buf).unwrap();
        let cursor = Cursor::new(buf);
        let bytes = cursor.get_ref().as_ref();
        let r_vec = self.parse_csv::<R>(bytes)?;
        Ok((r_vec, zip_file.name().to_string()))
    }
}
