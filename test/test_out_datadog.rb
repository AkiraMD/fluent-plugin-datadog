require 'test/unit'
require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'fluent/plugin/out_datadog'

class DatadogOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  BASE_CONFIG = %(
    api_key test_api_key
    tcp_ping_rate 0
  )

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf = BASE_CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::DatadogOutput).configure(conf)
  end

  class FakeChunk
    def initialize(events)
      @events = events
    end

    def msgpack_each(&block)
      @events.each(&block)
    end
  end

  def test_configure_defaults
    d = create_driver
    assert_equal 'test_api_key', d.instance.api_key
    assert_equal 'intake.logs.datadoghq.com', d.instance.host
    assert_equal true, d.instance.use_ssl
    assert_equal true, d.instance.ssl_verify
    assert_nil d.instance.ssl_ca_file
  end

  def test_configure_invalid_ssl_ca_file_raises
    assert_raise(Fluent::ConfigError) do
      create_driver(%(
        api_key test_api_key
        tcp_ping_rate 0
        ssl_ca_file /nonexistent/path/ca.pem
      ))
    end
  end

  def test_write_builds_message_with_api_key_and_json
    d = create_driver
    sent = []
    d.instance.define_singleton_method(:send_to_datadog) { |events| sent.concat(events) }

    chunk = FakeChunk.new([['test.tag', Time.now.to_f, { 'message' => 'hello world' }]])
    d.instance.write(chunk)

    assert_equal 1, sent.size
    assert_match(/\Atest_api_key /, sent.first)
    assert_match(/"message":"hello world"/, sent.first)
    assert sent.first.end_with?("\n")
  end

  def test_write_non_json_mode_requires_message_key
    d = create_driver(%(
      api_key test_api_key
      tcp_ping_rate 0
      use_json false
    ))
    sent = []
    d.instance.define_singleton_method(:send_to_datadog) { |events| sent.concat(events) }

    chunk = FakeChunk.new([
      ['test.tag', Time.now.to_f, { 'no_message' => 'skipped' }],
      ['test.tag', Time.now.to_f, { 'message' => 'kept' }]
    ])
    d.instance.write(chunk)

    assert_equal 1, sent.size
    assert_equal "test_api_key kept\n", sent.first
  end

  def test_container_tags_kubernetes_and_docker
    d = create_driver
    tags = d.instance.get_container_tags(
      'kubernetes' => {
        'container_image' => 'img',
        'container_name' => 'c',
        'namespace_name'  => 'ns',
        'pod_name'        => 'pod'
      },
      'docker' => { 'container_id' => 'abc123' }
    )
    assert_match(/image_name:img/, tags)
    assert_match(/container_name:c/, tags)
    assert_match(/kube_namespace:ns/, tags)
    assert_match(/pod_name:pod/, tags)
    assert_match(/container_id:abc123/, tags)
  end

  def test_end_to_end_format_and_write_via_driver
    # Exercises the full buffered pipeline: format -> buffer -> write.
    # flush_at_shutdown ensures the chunk is flushed before run returns.
    d = create_driver(%(
      api_key test_api_key
      tcp_ping_rate 0
      <buffer>
        flush_at_shutdown true
        flush_mode immediate
      </buffer>
    ))
    sent = []
    d.instance.define_singleton_method(:send_to_datadog) { |events| sent.concat(events) }

    d.run(default_tag: 'test.tag', shutdown: true, flush: true, wait_flush_completion: true) do
      d.feed(event_time, 'message' => 'via driver')
    end

    assert_equal 1, sent.size
    assert_match(/\Atest_api_key /, sent.first)
    assert_match(/"message":"via driver"/, sent.first)
  end

  def test_container_tags_empty_when_absent
    d = create_driver
    assert_equal '', d.instance.get_container_tags({})
  end
end
