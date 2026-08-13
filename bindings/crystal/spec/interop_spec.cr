require "./spec_helper"

describe "pyzmq interop" do
  it "sends from OMQ.cr to pyzmq" do
    have_pyzmq = Process.run(
      "python3",
      ["-c", "import zmq"],
      output: Process::Redirect::Close,
      error: Process::Redirect::Close
    ).success?
    pending!("pyzmq not installed") unless have_pyzmq

    base = File.tempname("omq-crystal")
    endpoint_file = "#{base}.endpoint"
    payload_file = "#{base}.payload"
    File.delete?(endpoint_file)
    File.delete?(payload_file)

    proc = Process.new(
      "python3",
      ["bindings/crystal/spec/pyzmq_pull_once.py", endpoint_file, payload_file],
      output: Process::Redirect::Inherit,
      error: Process::Redirect::Inherit
    )

    endpoint = wait_for_file(endpoint_file, 5.seconds)
    ctx = OMQ.context
    push = ctx.socket("push", linger: 1000, send_timeout: 1000)
    push.connect(endpoint)
    push.send("hello-pyzmq")
    push.close
    ctx.term

    proc.wait.success?.should be_true
    File.read(payload_file).should eq("hello-pyzmq")
  ensure
    File.delete?(endpoint_file) if endpoint_file
    File.delete?(payload_file) if payload_file
  end
end

private def wait_for_file(path : String, timeout : Time::Span) : String
  deadline = Time.instant + timeout
  while Time.instant < deadline
    if File.exists?(path)
      content = File.read(path)
      return content unless content.empty?
    end
    sleep 50.milliseconds
  end
  raise "timed out waiting for #{path}"
end
