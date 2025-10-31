use std::path::{Path, PathBuf};
use std::{fs, io};

pub fn read_file<P: AsRef<Path>>(file_name: P) -> String {
    fs::read_to_string(&file_name)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", file_name.as_ref().display()))
}

pub fn find(root: &Path, ext: &str) -> io::Result<Vec<PathBuf>> {
    let mut res = Vec::new();

    fn walk(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) -> io::Result<()> {
        for x in fs::read_dir(dir)? {
            let entry = x?;
            let path = entry.path();
            if path.is_dir() {
                walk(&path, ext, out)?;
            } else if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if let Some(e) = Path::new(name).extension().and_then(|s| s.to_str()) {
                    let wanted = ext.trim_start_matches('.');
                    if e == wanted {
                        let mut base = path.clone();
                        base.set_extension("");
                        out.push(base);
                    }
                }
            }
        }
        Ok(())
    }

    walk(root, ext, &mut res)?;
    Ok(res)
}
