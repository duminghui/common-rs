use std::path::{Path, PathBuf};
use std::{fs, io};

use eyre::eyre;

use crate::AResult;

pub fn unzip_single_file(path: impl AsRef<Path>, output_dir: impl AsRef<Path>) -> AResult<PathBuf> {
    let path_str = path.as_ref().display();
    let file = fs::File::open(&path).map_err(|e| eyre!("读取文件失败: {} {}", path_str, e))?;

    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| eyre!("解压文件失败: {} {}", path_str, e))?;
    let archive_file_count = archive.len();
    if archive_file_count != 1 {
        Err(eyre!(
            "压缩文件中文件大于1个: #{}# {}",
            archive_file_count,
            path.as_ref().display()
        ))?;
    }

    let mut zip_file = archive
        .by_index(0)
        .map_err(|e| eyre!("获取压缩包内文件失败: {} {}", path.as_ref().display(), e))?;

    let archive_outfile = zip_file
        .enclosed_name()
        .ok_or(eyre!("获取压缩包文件失败2: {}", path_str))?;

    let outfile_path = &output_dir.as_ref().join(archive_outfile);

    let mut outfile = fs::File::create(outfile_path)
        .map_err(|e| eyre!("创建解压目标文件失败: {} {}", outfile_path.display(), e))?;

    io::copy(&mut zip_file, &mut outfile)
        .map_err(|e| eyre!("写入解压目标文件失败: {} {}", outfile_path.display(), e))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Some(mode) = zip_file.unix_mode() {
            fs::set_permissions(outfile_path, fs::Permissions::from_mode(mode))
                .map_err(|e| eyre!("写入文件权限失败: {} {}", outfile_path.display(), e))?;
        }
    }

    Ok((*outfile_path).clone())
}
