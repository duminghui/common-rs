use std::borrow::Cow;
use std::env;
use std::path::Path;

use sysinfo::ProcessRefreshKind;

#[cfg(windows)]
fn name_wrapper(name: &str) -> Cow<'_, str> {
    if name.ends_with(".exe") {
        Cow::Borrowed(name)
    } else {
        Cow::Owned(format!("{}.exe", name))
    }
}

#[cfg(not(windows))]
fn name_wrapper(name: &str) -> Cow<'_, str> {
    Cow::Borrowed(name)
}

fn porcesses_by_name_count(name: &str) -> usize {
    let mut sys = sysinfo::System::default();
    sys.refresh_processes_specifics(ProcessRefreshKind::default());
    let name = name_wrapper(name);
    let processes = sys.processes_by_name(&name).collect::<Vec<_>>();
    processes.len()
}

pub fn app(name: &str) -> bool {
    porcesses_by_name_count(name) > 0
}

pub fn apps<'a>(names: &'a [&'a str]) -> Option<Cow<'a, str>> {
    for name in names {
        if app(name) {
            return Some(Cow::Borrowed(name));
        }
    }
    None
}

pub fn app_self() -> bool {
    let mut args = env::args();
    let cmd = args.next().unwrap();
    let name = Path::new(&cmd).file_name().unwrap().to_str().unwrap();
    porcesses_by_name_count(name) > 1
}
