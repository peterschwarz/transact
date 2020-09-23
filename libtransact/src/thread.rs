pub struct PanicMonitor {
    name: String,
}

impl PanicMonitor {
    pub fn new(name: String) -> Self {
        info!("Monitoring thread {}", name);
        Self { name }
    }
}

impl Drop for PanicMonitor {
    fn drop(&mut self) {
        if std::thread::panicking() {
            error!("Thread {} panicked", self.name);
            std::process::exit(-1);
        } else {
            warn!("Thread {} exited normally", self.name);
        }
    }
}
