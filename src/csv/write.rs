use std::io::Write;

use rayon::iter::{IntoParallelIterator, ParallelExtend, ParallelIterator};

use super::contention_pool::LowContentionPool;
use super::POOL;
use crate::AResult;

pub trait CsvRow {
    fn csv_row(&self) -> String;
}

pub struct CsvWriter<W: Write> {
    /// File or Stream handler
    buffer:              W,
    header:              Option<Vec<String>>,
    bom:                 bool,
    batch_size:          usize,
    n_threads:           usize,
    /// Used as separator.
    pub separator:       u8,
    /// String appended after every row.
    pub line_terminator: String,
}

impl<W: Write> CsvWriter<W>
where
    W: Write,
{
    pub fn new(buffer: W) -> Self {
        CsvWriter {
            buffer,
            header: None,
            bom: false,
            batch_size: 1024,
            n_threads: POOL.current_num_threads(),
            separator: b',',
            line_terminator: "\n".into(),
        }
    }

    pub fn with_header(mut self, header: &[&str]) -> Self {
        let header = header.iter().map(|v| v.to_string()).collect::<Vec<_>>();
        self.header = Some(header);
        self
    }

    /// Writes a CSV header to `writer`.
    fn write_header(&mut self) -> AResult<()> {
        if let Some(header) = &self.header {
            let header = header.join(&self.separator.to_string());
            self.buffer.write_all(header.as_bytes())?;
            self.buffer.write_all(self.line_terminator.as_bytes())?;
        }
        Ok(())
    }

    /// Writes a UTF-8 BOM to `writer`.
    fn write_bom(&mut self) -> AResult<()> {
        const BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];
        self.buffer.write_all(&BOM)?;
        Ok(())
    }

    fn write<T>(&mut self, datas: &[T]) -> AResult<()>
    where
        T: CsvRow + Sync,
    {
        let len = datas.len();
        let chunk_size = self.batch_size;
        let n_threads = self.n_threads;
        let total_rows_per_pool_iter = n_threads * chunk_size;
        let write_buffer_pool = LowContentionPool::<Vec<_>>::new(n_threads);

        let mut n_rows_finished = 0;

        let mut result_buf = Vec::<Vec<u8>>::with_capacity(n_threads);

        while n_rows_finished < len {
            let buf_writer = |thread_no: usize| {
                let thread_offset = thread_no * chunk_size;
                let start_offset = n_rows_finished + thread_offset;
                let stop_offset = start_offset + chunk_size;
                let clamped_start_offset = start_offset.clamp(0, len);
                let clamped_stop_offset = stop_offset.clamp(0, len);

                let datas = &datas[clamped_start_offset..clamped_stop_offset];

                let mut write_buffer = write_buffer_pool.get();

                if datas.is_empty() {
                    return write_buffer;
                }

                for data in datas {
                    write!(write_buffer, "{}", data.csv_row()).unwrap();
                    write_buffer.extend_from_slice("\n".as_bytes());
                }

                write_buffer
            };

            if n_threads > 1 {
                let par_iter = (0..n_threads).into_par_iter().map(buf_writer);
                POOL.install(|| result_buf.par_extend(par_iter));
            } else {
                result_buf.push(buf_writer(0));
            }

            for mut buf in result_buf.drain(..) {
                self.buffer.write_all(&buf).unwrap();
                buf.clear();
                write_buffer_pool.set(buf);
            }
            n_rows_finished += total_rows_per_pool_iter;
        }
        // self.buffer.flush()?;
        Ok(())
    }

    pub fn finish<T>(&mut self, datas: &[T]) -> AResult<()>
    where
        T: CsvRow + Sync,
    {
        if self.bom {
            self.write_bom()?;
        }
        self.write_header()?;
        self.write(datas)?;
        Ok(())
    }
}
