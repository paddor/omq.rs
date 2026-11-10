use super::*;
use std::collections::HashMap;
use std::net::IpAddr;

static ANSWERS: LazyLock<Mutex<HashMap<u16, Answer>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy)]
enum Reply {
    Address(IpAddr),
    /// The lookup succeeds with no addresses.
    Empty,
    Fail,
}

impl From<Option<IpAddr>> for Reply {
    fn from(address: Option<IpAddr>) -> Self {
        address.map_or(Self::Fail, Self::Address)
    }
}

struct Answer {
    reply: Reply,
    calls: usize,
}

#[derive(Debug)]
pub(crate) struct ScopedLookup {
    port: u16,
}

impl ScopedLookup {
    /// Answer lookups on `port` with `address`, or fail them for `None`.
    pub(crate) fn new(port: u16, address: Option<IpAddr>) -> Self {
        let answer = Answer {
            reply: address.into(),
            calls: 0,
        };
        let previous = ANSWERS.lock().unwrap().insert(port, answer);
        assert!(previous.is_none(), "test DNS port already in use");

        Self { port }
    }

    pub(crate) fn set_address(&self, address: Option<IpAddr>) {
        self.set(address.into());
    }

    /// Answer later lookups with an empty address list.
    pub(crate) fn set_empty(&self) {
        self.set(Reply::Empty);
    }

    fn set(&self, reply: Reply) {
        ANSWERS.lock().unwrap().get_mut(&self.port).unwrap().reply = reply;
    }

    pub(crate) fn calls(&self) -> usize {
        ANSWERS.lock().unwrap().get(&self.port).unwrap().calls
    }
}

impl Drop for ScopedLookup {
    fn drop(&mut self) {
        ANSWERS.lock().unwrap().remove(&self.port);
    }
}

pub(super) fn resolve(port: u16) -> Result<Vec<SocketAddr>> {
    let mut answers = ANSWERS.lock().unwrap();
    let answer = answers.get_mut(&port).expect("test DNS answer missing");
    answer.calls += 1;

    match answer.reply {
        Reply::Address(address) => Ok(vec![SocketAddr::new(address, port)]),
        Reply::Empty => Ok(Vec::new()),
        Reply::Fail => Err(Error::Io(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "test DNS failure",
        ))),
    }
}
