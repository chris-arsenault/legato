//! Build-time WinFSP linker configuration for Windows package builds.

fn main() {
    #[cfg(windows)]
    winfsp::build::winfsp_link_delayload();
}
