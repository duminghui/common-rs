use std::fmt;

use eyre::eyre;

pub trait EyreExt<T> {
    #[track_caller]
    fn eyre(self) -> Result<T, eyre::Error>;
    #[track_caller]
    fn eyre_with_msg<M>(self, message: M) -> Result<T, eyre::Error>
    where
        M: fmt::Display;
}

impl<T, E> EyreExt<T> for Result<T, E>
where
    E: fmt::Display + fmt::Debug + Send + Sync + 'static,
{
    fn eyre(self) -> Result<T, eyre::Error> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => Err(eyre!(e)),
        }
    }

    fn eyre_with_msg<M>(self, message: M) -> Result<T, eyre::Error>
    where
        M: fmt::Display,
    {
        match self {
            Ok(t) => Ok(t),
            Err(e) => Err(eyre!("{}, err:{}", message, e)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::EyreExt;

    #[test]
    fn test_1() {
        let a = None::<String>;
        let a = a.ok_or("xxx").eyre();

        println!("{:?}", a.err().unwrap());

        let a = None::<String>;
        let a = a.ok_or("xxx").eyre_with_msg("BBBBBBBB");
        println!("{:?}", a.err().unwrap());
    }
}
