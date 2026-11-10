/// Poll `future` once with a no-op waker.
pub(crate) fn poll_once<F: Future>(future: F) -> std::task::Poll<F::Output> {
    let waker = std::task::Waker::noop();
    std::pin::pin!(future).poll(&mut std::task::Context::from_waker(waker))
}
