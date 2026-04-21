# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache License Version 2.0.
# This product includes software developed at Datadog (https://www.datadoghq.com/).
# Copyright 2018 Datadog, Inc.

require 'socket'
require 'openssl'
require 'json'
require 'time'

module Fluent
  module Plugin
    class DatadogOutput < Fluent::Plugin::Output
      class ConnectionFailure < StandardError; end

      Fluent::Plugin.register_output('datadog', self)

      helpers :compat_parameters

      # Output settings
      config_param :use_json,           :bool,    default: true
      config_param :include_tag_key,    :bool,    default: false
      config_param :tag_key,            :string,  default: 'tag'
      config_param :timestamp_key,      :string,  default: '@timestamp'
      config_param :service,            :string,  default: nil
      config_param :dd_sourcecategory,  :string,  default: nil
      config_param :dd_source,          :string,  default: nil
      config_param :dd_tags,            :string,  default: nil
      config_param :dd_hostname,        :string,  default: nil

      # Connection settings
      config_param :host,           :string,  default: 'intake.logs.datadoghq.com'
      config_param :use_ssl,        :bool,    default: true
      config_param :port,           :integer, default: 10514
      config_param :ssl_port,       :integer, default: 10516
      config_param :max_retries,    :integer, default: -1
      config_param :tcp_ping_rate,  :integer, default: 10

      # API Settings
      config_param :api_key, :string

      config_section :buffer do
        config_set_default :@type, 'memory'
      end

      def configure(conf)
        compat_parameters_convert(conf, :buffer)
        super
        return if @dd_hostname

        @dd_hostname = %x[hostname -f 2> /dev/null].strip
        @dd_hostname = Socket.gethostname if @dd_hostname.empty?
      end

      def multi_workers_ready?
        true
      end

      def new_client
        if @use_ssl
          context    = OpenSSL::SSL::SSLContext.new
          socket     = TCPSocket.new @host, @ssl_port
          ssl_client = OpenSSL::SSL::SSLSocket.new socket, context
          ssl_client.connect
          ssl_client
        else
          TCPSocket.new @host, @port
        end
      end

      def start
        super
        @my_mutex = Mutex.new
        @running = true

        if @tcp_ping_rate > 0
          @timer = Thread.new do
            while @running
              send_to_datadog(["fp\n"])
              sleep(@tcp_ping_rate)
            end
          end
        end
      end

      def shutdown
        @running = false
        @client&.close
        super
      end

      # This method is called when an event reaches Fluentd.
      def format(tag, time, record)
        # When Fluent::EventTime is msgpack'ed it gets converted to int with seconds
        # precision only. We explicitly convert it to floating point number, which
        # is compatible with Time.at below.
        [tag, time.to_f, record].to_msgpack
      end

      # NOTE! This method is called by internal thread, not Fluentd's main thread.
      # 'chunk' is a buffer chunk that includes multiple formatted events.
      def write(chunk)
        messages = []

        chunk.msgpack_each do |tag, time, record|
          next unless record.is_a?(Hash)
          next if record.empty?

          record["ddsourcecategory"] ||= @dd_sourcecategory if @dd_sourcecategory
          record["ddsource"]         ||= @dd_source         if @dd_source
          record["ddtags"]           ||= @dd_tags           if @dd_tags
          record["service"]          ||= @service           if @service
          record["hostname"]         ||= @dd_hostname       if @dd_hostname

          record[@tag_key] = tag if @include_tag_key

          # If @timestamp_key already exists, we don't overwrite it.
          if @timestamp_key && record[@timestamp_key].nil? && time
            record[@timestamp_key] = Time.at(time).utc.iso8601(3)
          end

          container_tags = get_container_tags(record)
          unless container_tags.empty?
            if record["ddtags"].nil? || record["ddtags"].empty?
              record["ddtags"] = container_tags
            else
              record["ddtags"] = record["ddtags"] + "," + container_tags
            end
          end

          if @use_json
            messages.push "#{@api_key} " + record.to_json + "\n"
          else
            next unless record.key?("message")
            messages.push "#{@api_key} " + record["message"].strip + "\n"
          end
        end
        send_to_datadog(messages)
      end

      def send_to_datadog(events)
        @my_mutex.synchronize do
          events.each do |event|
            log.trace "Datadog plugin: about to send event=#{event}"
            retries = 0
            begin
              log.info "New attempt to Datadog attempt=#{retries}" if retries > 1
              @client ||= new_client
              @client.write(event)
            rescue => e
              @client.close rescue nil
              @client = nil

              if retries == 0
                # immediately retry, in case it's just a server-side close
                retries += 1
                retry
              end

              if retries < @max_retries || @max_retries == -1
                a_couple_of_seconds = [retries ** 2, 30].min
                retries += 1
                log.warn "Could not push event to Datadog, attempt=#{retries} max_attempts=#{@max_retries} wait=#{a_couple_of_seconds}s error=#{e}"
                sleep a_couple_of_seconds
                retry
              end
              raise ConnectionFailure, "Could not push event to Datadog after #{retries} retries, #{e}"
            end
          end
        end
      end

      # Collect docker and kubernetes tags for your logs using `filter_kubernetes_metadata` plugin,
      # for more information about the attribute names, check:
      # https://github.com/fabric8io/fluent-plugin-kubernetes_metadata_filter/blob/master/lib/fluent/plugin/filter_kubernetes_metadata.rb#L265

      def get_container_tags(record)
        [
          get_kubernetes_tags(record),
          get_docker_tags(record)
        ].compact.join(",")
      end

      def get_kubernetes_tags(record)
        if record.key?('kubernetes') && !record.fetch('kubernetes').nil?
          kubernetes = record['kubernetes']
          tags = []
          tags.push("image_name:" + kubernetes['container_image']) unless kubernetes['container_image'].nil?
          tags.push("container_name:" + kubernetes['container_name']) unless kubernetes['container_name'].nil?
          tags.push("kube_namespace:" + kubernetes['namespace_name']) unless kubernetes['namespace_name'].nil?
          tags.push("pod_name:" + kubernetes['pod_name']) unless kubernetes['pod_name'].nil?
          tags.join(",")
        end
      end

      def get_docker_tags(record)
        if record.key?('docker') && !record.fetch('docker').nil?
          docker = record['docker']
          tags = []
          tags.push("container_id:" + docker['container_id']) unless docker['container_id'].nil?
          tags.join(",")
        end
      end
    end
  end
end
