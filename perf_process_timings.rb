require 'optparse'

def delta_timings(current_timings, last_timings)
  new_timings = {}
  current_timings.each do |metric, current_value|
    if /^num_.*/.match(metric)
      new_timings[metric] = current_value
    else
      new_timings[metric] = last_timings.has_key?(metric) ? current_value - last_timings[metric] : 0
    end
  end
  new_timings
end

def put_timings(o, timings)
  unless timings.nil?
    timings.keys.each do |metric|
      next if /total_time/.match(metric)
      format = /^num_.*/.match(metric) ? "%-36s %5.6f" : "%-36s %5.6f seconds"
      o.puts "  #{sprintf(format, "#{metric}:", timings[metric])}" unless timings[metric].zero?
    end
    # print total_time last in each section
    if timings.has_key?(:total_time)
      o.puts "  #{sprintf("%-36s %5.6f seconds", "total_time:", timings[:total_time])}" unless timings[:total_time].zero?
    end
  end
end

def update_timings(last_timings, current_timings)
  updated_timings = last_timings
  current_timings.each do |metric, timing|
    updated_timings[metric] = timing
  end
  updated_timings
end
      
def stats (workers, options)
  # Full counters: :capture_state,:total_time,:vim_connect,:capture_intervals,:capture_counters,:build_query_params,
  #                :vim_execute_time,:perf_processing,:rhevm_connect,:collect_data,:connect,:capture_counter_values
  # Unique counters:
  unique_perf_capture_counters    = [:vim_connect,:capture_intervals,:capture_counters,
                                     :build_query_params,:vim_execute_time,:perf_processing,:rhevm_connect,
                                     :collect_data,:connect,:capture_counter_values,:num_vim_queries,:num_vim_trips]
  
  # Full counters: :db_find_storage_files,:capture_state,:init_attrs,:db_find_prev_perfs,:process_perfs,
  #                :process_perfs_tag,:process_bottleneck,:total_time
  # Unique counters:
  unique_storage_capture_counters = [:db_find_storage_files,:init_attrs,:process_bottleneck]
  
  # Full counters: :process_counter_values,:db_find_prev_perfs,:process_perfs,:process_perfs_db,
  #                :process_perfs_tag,:add_missing_intervals
  # Unique counters:
  unique_process_counters         = [:process_counter_values,:process_perfs_db,:add_missing_intervals]
  other_counters                  = [:server_dequeue,:heartbeat,:server_monitor,:log_active_servers,:worker_monitor,:worker_dequeue]
  
  if options[:outputfile]
    o = File.open(options[:outputfile],'w')
  else
    o = $stdout.dup
  end

  workers.each do |pid, messages|
    last_timings = {}
    messages.each do |perf_process|
      o.puts "---"
      o.puts "Worker PID:                    #{pid}"
      o.puts "Message ID:                    #{perf_process[:message_id]}"
      o.puts "Message fetch time:            #{perf_process[:message_time]}"
      o.puts "Message time in queue:         #{perf_process[:message_dequeue_time]} seconds"
      o.puts "Provider:                      #{perf_process[:provider]}"
      o.puts "Object type:                   #{perf_process[:object]}"
      o.puts "Object name:                   #{perf_process[:obj_name]}"
      o.puts "Metrics processing start time: #{perf_process[:start_time]}"
      o.puts "Time range:                    #{perf_process[:range]}"
      o.puts "Rows added:                    #{perf_process[:num_added]}"
      o.puts "Rows updated:                  #{perf_process[:num_updated]}"
      o.puts "Capture state:                 #{perf_process[:capture_state]}"
      case perf_process[:capture_state]
      when 'capture_started'
        o.puts "Capture started but incomplete"
        # TODO: re-factor
      when 'perf_capture_complete'
        o.puts "Capture timings:"
        perf_capture_timings = eval(perf_process[:capture_timings]) if @timings_re.match(perf_process[:capture_timings])
        unless perf_capture_timings.nil?
          if (perf_capture_timings.keys & (unique_storage_capture_counters + unique_process_counters + other_counters)).any?
            # o.puts "*** Debug - BZ1424716 ***"
            perf_capture_counters = unique_perf_capture_counters + [:total_time, :capture_state]
            perf_capture_timings.delete_if { |key, _| !perf_capture_counters.include?(key) }
              put_timings(o, delta_timings(perf_capture_timings,last_timings))
          else
            put_timings(o, perf_capture_timings)
          end
          last_timings = update_timings(last_timings, perf_capture_timings)
        end
      when 'storage_capture_complete'
        o.puts "Capture timings:"
        storage_capture_timings = eval(perf_process[:capture_timings]) if @timings_re.match(perf_process[:capture_timings])
        unless storage_capture_timings.nil?
          if (storage_capture_timings.keys & (unique_perf_capture_counters + unique_process_counters + other_counters)).any?
            # o.puts "*** Debug - BZ1424716 ***"
            # Need to delete the erroneous counters then subtract previous counters from the remainder (https://bugzilla.redhat.com/show_bug.cgi?id=1424716)
            storage_capture_counters = unique_storage_capture_counters + [:total_time, :process_perfs, :db_find_prev_perfs, :process_perfs_tag, :capture_state]
            storage_capture_timings.delete_if { |key, _| !storage_capture_counters.include?(key) }
            put_timings(o, delta_timings(storage_capture_timings,last_timings))
          else
            put_timings(o, storage_capture_timings)
          end
          last_timings = update_timings(last_timings, storage_capture_timings)
        end
        o.puts "Metrics processing end time:   #{perf_process[:end_time]}"
      when 'collect_error'
        o.puts "Capture timings at time of error:"
        error_timings = eval(perf_process[:error_timings]) if @timings_re.match(perf_process[:error_timings])
        put_timings(o, delta_timings(error_timings,last_timings))
        update_timings(last_timings, error_timings)
      end
      if perf_process[:process_state] == 'process_skipped'
        o.puts "** No metrics were captured **"
      end
      if perf_process[:process_state] == 'process_complete'
        o.puts "Process timings:"
        process_timings = eval(perf_process[:process_timings]) if @timings_re.match(perf_process[:process_timings])
        unless process_timings.nil?
          if (process_timings.keys & (unique_perf_capture_counters + unique_storage_capture_counters + other_counters)).any?
            # o.puts "*** Debug - BZ1424716 ***"
            # Need to delete the erroneous counters then subtract previous counters from the remainder (https://bugzilla.redhat.com/show_bug.cgi?id=1424716)
            process_counters = unique_process_counters + [:total_time, :process_perfs, :process_perfs_tag, :db_find_prev_perfs]
            process_timings.delete_if { |key, _| !process_counters.include?(key) }
              put_timings(o, delta_timings(process_timings,last_timings))
          else
            put_timings(o, process_timings)
          end
          update_timings(last_timings, process_timings)
        end
        o.puts "Metrics processing end time:   #{perf_process[:end_time]}"
      end
      o.puts "Message delivered time:        #{perf_process[:message_delivered_time]}"
      o.puts "Message state:                 #{perf_process[:message_state]}"
      o.puts "Message delivered in:          #{perf_process[:message_delivered_in]} seconds"
      o.puts "---"
      o.puts ""
    end
  end
end

# [----] I, [2017-02-21T13:15:00.616554 #18254:33d13c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::MetricsCollectorWorker::Runner#get_message_via_drb) Message id: [764554], MiqWorker id: [2888], Zone: [default], Role: [ems_metrics_collector], Server: [], Ident: [redhat], Target id: [], Instance id: [61], Task id: [], Command: [ManageIQ::Providers::Redhat::InfraManager::Vm.perf_capture_realtime], Timeout: [600], Priority: [100], State: [dequeue], Deliver On: [], Data: [], Args: [], Dequeued in: [13.405617295] seconds

get_message_via_drb_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\]
                          \ .*get_message_via_drb\)
                          \ Message\ id:\ \[(?<message_id>\d+)\],
                          \ MiqWorker\ id:\ \[(?<worker_id>\h*)\],
                          \ Zone:\ \[(?<zone>.*)\],
                          \ Role:\ \[(?<role>.*)\],
                          (\ Server:\ \[(?<server>.*)\],)?
                          \ Ident:\ \[(?<ident>.*)\],
                          \ Target\ id:\ \[(?<target_id>.*)\],
                          \ Instance\ id:\ \[(?<instance_id>.*)\],
                          \ Task\ id:\ \[(?<task_id>.*)\],
                          \ Command:\ \[(?<command>.*)\],
                          \ Timeout:\ \[(?<timeout>\d*)\],
                          \ Priority:\ \[(?<priority>\d*)\],
                          \ State:\ \[(?<state>\w+)\],
                          \ Deliver\ On:\ \[(?<deliver_on>.*)\],
                          \ Data:\ \[(?<data>.*)\],
                          \ Args:\ \[(?<args>.*)\],
                          \ Dequeued\ in:\ \[(?<dequeued_in>.+)\]\ seconds$
                            }x


# [----] I, [2017-03-02T18:52:47.477331 #584:925130]  INFO -- : MIQ(ManageIQ::Providers::Openstack::CloudManager::Vm#perf_capture) [realtime] Capture for ManageIQ::Providers::Openstack::CloudManager::Vm name: [ogranit-cfme], id: [284]...
# CFME 5.8
# [----] I, [2017-04-25T11:00:22.609665 #26762:106312c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Vm#perf_capture) [realtime] Capture for ManageIQ::Providers::Redhat::InfraManager::Vm name: [jst-web01], id: [15], start_time: [2017-04-25 00:00:00 UTC]...

perf_capture_start_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_capture\)\ \[realtime\]\ Capture\ for\ ManageIQ::Providers::(?<provider>.+)::(?<object>.+)
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\](:?.*)\.\.\.$
                            }x

# [----] I, [2017-01-30T01:02:12.823564 #3200:66b14c]  INFO -- : MIQ(Storage#perf_capture) [hourly] Capture for Storage name: [DAS2_SGOP042], id: [1000000000114]...

storage_capture_start_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_capture\)\ \[hourly\]\ Capture\ for\ Storage
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\]\.\.\.$
                            }x

# [----] I, [2017-02-20T14:14:42.669369 #18263:33d13c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Vm#perf_capture) [realtime] Capture for ManageIQ::Providers::Redhat::InfraManager::Vm name: [lampsrv01], id: [61]...Complete - Timings: {:heartbeat=>0.03189492225646973, :server_dequeue=>0.003981828689575195, :capture_state=>6.216104030609131, :rhevm_connect=>0.7932844161987305, :collect_data=>11.26729440689087, :total_time=>484.49506402015686, :process_counter_values=>0.8874821662902832, :db_find_prev_perfs=>0.7818803787231445, :process_perfs=>62.527793884277344, :process_perfs_db=>382.08203387260437, :db_find_storage_files=>0.41226696968078613, :init_attrs=>0.41890859603881836, :process_perfs_tag=>0.03686666488647461, :process_bottleneck=>10.27318787574768}
# Sometimes it's correct, as follows:
# [----] I, [2017-01-25T03:22:25.940788 #305:66b14c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Vm#perf_capture) [realtime] Capture for ManageIQ::Providers::Vmware::InfraManager::Vm name: [VERD452], id: [1000000000475]...Complete - Timings: {:capture_state=>0.01109933853149414, :vim_connect=>0.043763160705566406, :capture_intervals=>0.06079673767089844, :capture_counters=>0.05180954933166504, :build_query_params=>0.0002880096435546875, :num_vim_queries=>1, :vim_execute_time=>0.10877013206481934, :perf_processing=>0.02622389793395996, :num_vim_trips=>1, :total_time=>0.3047974109649658}
# CFME 5.8
# [----] I, [2017-04-25T11:00:23.428229 #26762:106312c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Vm#perf_capture) [realtime] Capture for ManageIQ::Providers::Redhat::InfraManager::Vm name: [jst-web01], id: [15], start_time: [2017-04-25 00:00:00 UTC]...Complete - Timings: {:capture_state=>0.07574152946472168, :rhevm_connect=>0.01647043228149414, :collect_data=>0.724677324295044, :total_time=>0.8183016777038574}

perf_capture_complete_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_capture\)\ \[realtime\]\ Capture\ for\ ManageIQ::Providers::(?<provider>.+)::(?<object>.+)
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\](:?.*)\.\.\.Complete\ -
                            \ Timings: (?<timings>.*$)
                            }x

# [----] I, [2017-01-28T03:09:11.370442 #19020:66b14c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Vm#perf_capture) [realtime] Skipping processing for ManageIQ::Providers::Vmware::InfraManager::Vm name: [VERD377], id: [1000000000698] as no metrics were captured.

perf_process_skipped_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_capture\)\ \[realtime\]\ Skipping\ processing\ for\ ManageIQ::Providers::(?<provider>.+)::(?<object>.+)
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\]
                            \ as\ no\ metrics\ were\ captured\.$
                            }x

# [----] I, [2017-01-30T01:02:20.773472 #3200:66b14c]  INFO -- : MIQ(Storage#perf_capture) [hourly] Capture for Storage name: [DAS2], id: [1000000000114]...Complete - Timings: {:server_dequeue=>0.003245830535888672, :capture_state=>22.106929302215576, :vim_connect=>44.42920637130737, :capture_intervals=>9.269381284713745, :capture_counters=>82.31622457504272, :build_query_params=>0.3026297092437744, :num_vim_queries=>1, :vim_execute_time=>155.0877604484558, :perf_processing=>31.102522373199463, :num_vim_trips=>1, :total_time=>1173.6877822875977, :process_counter_values=>21.14705777168274, :db_find_prev_perfs=>5.793952941894531, :process_perfs=>126.75096321105957, :process_perfs_db=>587.8275690078735, :db_find_storage_files=>2.852144718170166, :init_attrs=>4.23950457572937, :process_perfs_tag=>2.8116538524627686, :process_bottleneck=>31.69999098777771}

storage_capture_complete_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_capture\)\ \[hourly\]\ Capture\ for\ Storage
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\]\.\.\.Complete\ -
                            \ Timings: (?<timings>.*$)
                            }x

# [----] I, [2017-02-19T14:41:31.584155 #18254:33d13c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Vm#perf_process) [realtime] Processing for ManageIQ::Providers::Redhat::InfraManager::Vm name: [cloudforms06.bit63.net], id: [4], for range [2017-02-18T03:03:18Z - 2017-02-19T14:42:03Z]...

perf_process_start_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_process\)\ \[realtime\]\ Processing\ for\ ManageIQ::Providers::(?<provider>.+)::(?<object>.+)
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\],
                            \ for\ range\ \[(?<range>.+)\]\.\.\.$
                            }x

# [----] E, [2017-03-01T07:16:46.753782 #55228:e6d138] ERROR -- : MIQ(ManageIQ::Providers::Openstack::CloudManager::MetricsCapture#perf_collect_metrics) [realtime] for: [ManageIQ::Providers::Openstack::CloudManager::Vm], [234], [n059.eng.bos.redhat.com]   Timings at time of error: {:server_dequeue=>0.0059583187103271484, :capture_state=>0.8445370197296143, :connect=>1111.769113779068, :total_time=>1020.3793663978577}

perf_collect_error_re = %r{
                          ----\]\ E,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\].*\#perf_collect_metrics\)
                          \ \[realtime\]\ for:\ \[ManageIQ::Providers::(?<provider>.+)::(?<object>.+?)\],
                          \ \[(?<obj_id>\d+)\],
                          \ \[(?<obj_name>.+?)\]
                          \ \ \ Timings\ at\ time\ of\ error:\ (?<timings>.*$)
                          }x                          

# [----] I, [2017-01-29T09:54:54.195921 #10702:66b14c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Vm#perf_process) [realtime] Processing 159 performance rows...Complete - Added 159 / Updated 0

perf_rows_complete_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\]
                          \ .*perf_process\)\ \[realtime\]\ Processing\ (?<num_rows>\d+)\ performance\ rows\.\.\.Complete
                          \ -\ Added\ (?<num_added>\d+)\ /
                          \ Updated\ (?<num_updated>\d+)
                          }x

# [----] I, [2016-12-13T06:45:43.171268 #3104:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::HostEsx#perf_process) [realtime] Processing for ManageIQ::Providers::Vmware::InfraManager::HostEsx name: [vs3.vi.grp.net], id: [1000000000042], for range [2016-12-13T04:45:40Z - 2016-12-13T05:45:20Z]...Complete - Timings: {:heartbeat=>0.014219522476196289, :server_dequeue=>0.006936788558959961, :capture_state=>8763.291134595871, :vim_connect=>314417.52912831306, :capture_intervals=>193884.57209587097, :capture_counters=>296209.3751807213, :build_query_params=>322.45843839645386, :num_vim_queries=>1, :vim_execute_time=>387732.9721496105, :perf_processing=>19080.591324090958, :num_vim_trips=>1, :total_time=>2013918.6065928936, :process_counter_values=>18276.25559949875, :db_find_prev_perfs=>3039.8460755348206, :process_perfs=>86263.12925457954, :process_perfs_db=>572791.01060009, :db_find_storage_files=>263.6958165168762, :init_attrs=>1335.182659626007, :process_perfs_tag=>3156.6503195762634, :process_bottleneck=>12043.107157468796}
# Sometimes it's correct, as follows:
# [----] I, [2017-01-25T03:22:25.569662 #305:66b14c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Vm#perf_process) [realtime] Processing for ManageIQ::Providers::Vmware::InfraManager::Vm name: [VERD431], id: [1000000000393], for range [2017-01-25T01:31:20Z - 2017-01-25T02:22:20Z]...Complete - Timings: {:process_counter_values=>0.024013280868530273, :db_find_prev_perfs=>0.010066509246826172, :process_perfs=>0.2171170711517334, :process_perfs_db=>3.7727270126342773, :total_time=>4.06266450881958}

perf_process_complete_re = %r{
                             ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                             \ \#(?<pid>\h+):\h+\]
                             \ .*perf_process\)\ \[realtime\]\ Processing\ for\ ManageIQ::Providers::(?<provider>.+)::(?<object>.+)
                             \ name:\ \[(?<obj_name>.+?)\],
                             \ id:\ \[(?<obj_id>\d+)\],
                             \ for\ range\ \[(?<range>.+)\]\.\.\.Complete\ -
                             \ Timings: (?<timings>.*$)
                             }x

# [----] I, [2016-12-13T03:43:24.621330 #21612:11e598c]  INFO -- : MIQ(MiqQueue#delivered) Message id: [1000032162564], State: [ok], Delivered in [3.849833806] seconds

message_delivered_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\]
                          \ .*MIQ\(MiqQueue\#delivered\)\ Message\ id:\ \[(?<message_id>\d+)\],
                          \ State:\ \[(?<state>\w+)\],
                          \ Delivered\ in\ \[(?<delivered_in>.+)\]\ seconds
                          }x

# Ensure we're not evaling dubious code

@timings_re = %r{
                \{
                (?::\w+=>\d+\.?[\de-]*,?\ ?)+
                \}
                }x

begin
  options = {:inputfile => nil, :outputfile => nil}
  
  parser = OptionParser.new do|opts|
    opts.banner = "Usage: perf_process_timings.rb [options]"
    opts.on('-i', '--inputfile inputfile', 'Full file path to evm.log (if not /var/www/miq/vmdb/log/evm.log)') do |inputfile|
      options[:inputfile] = inputfile;
    end
    opts.on('-o', '--outputfile outputfile', 'Full file path to optional output file') do |outputfile|
      options[:outputfile] = outputfile;
    end
    opts.on('-h', '--help', 'Displays Help') do
      puts opts
      exit
    end
  end
  parser.parse!

  if options[:inputfile].nil?
    inputfile = "/var/www/miq/vmdb/log/evm.log"
  else
    inputfile = options[:inputfile]
  end

  messages = {}
  workers = {}
  counter = 0
  $stdout.sync = true
  File.foreach( inputfile ) do |line|

    new_message = get_message_via_drb_re.match(line)
    if new_message
      messages[new_message[:pid]] = {:timestamp   => new_message[:timestamp], 
                                     :dequeued_in => new_message[:dequeued_in],
                                     :message_id  => new_message[:message_id]}
      next
    end

    started = perf_capture_start_re.match(line) || storage_capture_start_re.match(line)
    if started
      counter += 1
      print "Found #{counter} processings of performance metrics\r"

      workers[started[:pid]] = [] unless workers.has_key?(started[:pid])
      workers[started[:pid]] << {:capture_state  => 'capture_started', 
                                     :obj_name   => started[:obj_name],
                                     :obj_id     => started[:obj_id],
                                     :start_time => started[:timestamp]}
      current = workers[started[:pid]].length - 1                             
      if started.names.include?('provider')
        workers[started[:pid]][current][:provider] = started[:provider]
        workers[started[:pid]][current][:object]   = started[:object]
      else
        workers[started[:pid]][current][:object]   = 'Storage'
        workers[started[:pid]][current][:range]    = 'Hourly'
      end
      if messages.has_key?(started[:pid])
        workers[started[:pid]][current][:message_id] = messages[started[:pid]][:message_id]
        workers[started[:pid]][current][:message_time] = messages[started[:pid]][:timestamp]
        workers[started[:pid]][current][:message_dequeue_time] = messages[started[:pid]][:dequeued_in]
        messages.delete(started[:pid])
      else
        workers[started[:pid]][current][:message_time] = "No message found"
        workers[started[:pid]][current][:message_dequeue_time] = ""
      end
      next
    end

    perf_capture_complete = perf_capture_complete_re.match(line)
    if perf_capture_complete
      next if workers[perf_capture_complete[:pid]].nil?
      current = workers[perf_capture_complete[:pid]].length - 1 rescue next
      workers[perf_capture_complete[:pid]][current][:capture_state] = 'perf_capture_complete'
      workers[perf_capture_complete[:pid]][current][:capture_timings] = perf_capture_complete[:timings]
      next
    end
    
    storage_capture_complete = storage_capture_complete_re.match(line)
    if storage_capture_complete
      next if workers[storage_capture_complete[:pid]].nil?
      current = workers[storage_capture_complete[:pid]].length - 1 rescue next
      workers[storage_capture_complete[:pid]][current][:capture_state] = 'storage_capture_complete'
      workers[storage_capture_complete[:pid]][current][:capture_timings] = storage_capture_complete[:timings]
      workers[storage_capture_complete[:pid]][current][:end_time] = storage_capture_complete[:timestamp]
      next
    end

    perf_collect_error = perf_collect_error_re.match(line)
    if perf_collect_error
      next if workers[perf_collect_error[:pid]].nil?
      current = workers[perf_collect_error[:pid]].length - 1 rescue next
      workers[perf_collect_error[:pid]][current][:capture_state] = 'collect_error'
      workers[perf_collect_error[:pid]][current][:end_time] = perf_collect_error[:timestamp]
      workers[perf_collect_error[:pid]][current][:error_timings] = perf_collect_error[:timings]
      next
    end

    perf_process_skipped = perf_process_skipped_re.match(line)
    if perf_process_skipped
      next if workers[perf_process_skipped[:pid]].nil?
      current = workers[perf_process_skipped[:pid]].length - 1 rescue next
      workers[perf_process_skipped[:pid]][current][:process_state] = 'process_skipped'
      workers[perf_process_skipped[:pid]][current][:end_time] = perf_process_skipped[:timestamp]
      next
    end

    rows_complete = perf_rows_complete_re.match(line)
    if rows_complete
      next if workers[rows_complete[:pid]].nil?
      current = workers[rows_complete[:pid]].length - 1
      workers[rows_complete[:pid]][current][:num_added] = rows_complete[:num_added]
      workers[rows_complete[:pid]][current][:num_updated] = rows_complete[:num_updated]
      next
    end

    process_complete = perf_process_complete_re.match(line)
    if process_complete
      next if workers[process_complete[:pid]].nil?
      current = workers[process_complete[:pid]].length - 1 rescue next
      workers[process_complete[:pid]][current][:process_state] = 'process_complete'
      workers[process_complete[:pid]][current][:end_time] = process_complete[:timestamp]
      workers[process_complete[:pid]][current][:process_timings] = process_complete[:timings]
      workers[process_complete[:pid]][current][:range] = process_complete[:range]
      next
    end

    message_delivered = message_delivered_re.match(line)
    if message_delivered
      next if workers[message_delivered[:pid]].nil?
      current = workers[message_delivered[:pid]].length - 1 rescue next
      if message_delivered[:message_id] == workers[message_delivered[:pid]][current][:message_id]
        workers[message_delivered[:pid]][current][:message_delivered_time] = message_delivered[:timestamp]
        workers[message_delivered[:pid]][current][:message_state] = message_delivered[:state]
        workers[message_delivered[:pid]][current][:message_delivered_in] = message_delivered[:delivered_in]
      end
      next
    end
  end
  stats(workers, options)
  puts ""

rescue => err
  puts "[#{err}]\n#{err.backtrace.join("\n")}"
  exit!
end




