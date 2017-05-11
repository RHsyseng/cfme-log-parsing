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
  
  common_non_rollup_counters = [:purge_metrics,:server_dequeue,:query_batch,:heartbeat,:server_monitor,:log_active_servers,:worker_monitor,:worker_dequeue]
  hourly_rollup_counters = [:db_find_prev_perf,:rollup_perfs,:db_update_perf,:process_perfs_tag,:total_time,:process_bottleneck]
  realtime_and_daily_rollup_counters  = [:db_find_prev_perf,:rollup_perfs,:db_update_perf,:process_perfs_tag,:total_time]

  # realtime:
  # non_rollup_counters = [:purge_metrics,:server_dequeue,:query_batch,:process_bottleneck,:heartbeat,:server_monitor,:log_active_servers,:worker_monitor,:worker_dequeue]
  # rollup_counters     = [:db_find_prev_perf,:rollup_perfs,:db_update_perf,:process_perfs_tag,:total_time]

  if options[:outputfile]
    o = File.open(options[:outputfile],'w')
  else
    o = $stdout.dup
  end

  workers.each do |pid, messages|
    last_timings = {}
    messages.each do |rollup|
      o.puts "---"
      o.puts "Worker PID:                    #{pid}"
      if rollup[:type] == "realtime"
        if rollup[:first_rollup_for_message]
          o.puts "Message ID:                    #{rollup[:message_id]} (new)"
          o.puts "Message fetch time:            #{rollup[:message_time]}"
          o.puts "Message time in queue:         #{rollup[:message_dequeue_time]} seconds"
        else
          o.puts "Message ID:                    #{rollup[:message_id]} (continued)"
        end
      else
        o.puts "Message ID:                    #{rollup[:message_id]}"
        o.puts "Message fetch time:            #{rollup[:message_time]}"
        o.puts "Message time in queue:         #{rollup[:message_dequeue_time]} seconds"
      end
      o.puts "Rollup processing start time:  #{rollup[:start_time]}"
      o.puts "Object Type:                   #{rollup[:obj_type]}"
      o.puts "Object Name:                   #{rollup[:obj_name]}"
      o.puts "Rollup Type:                   #{rollup[:type]}"
      if rollup[:type] == "hourly"
        rollup_counters = hourly_rollup_counters
        non_rollup_counters = common_non_rollup_counters
      else
        rollup_counters = realtime_and_daily_rollup_counters
        non_rollup_counters = common_non_rollup_counters + [:process_bottleneck]
      end
      o.puts "Time:                          #{rollup[:time]}"
      o.puts "Rollup timings:"
      rollup_timings = eval(rollup[:timings]) if @timings_re.match(rollup[:timings])
      unless rollup_timings.nil?
        if (rollup_timings.keys & (non_rollup_counters)).any?
          # o.puts "*** Debug - BZ1424716 ***"
          # Need to delete the erroneous counters then subtract previous counters from the remainder (https://bugzilla.redhat.com/show_bug.cgi?id=1424716)
          rollup_timings.delete_if { |key, _| !rollup_counters.include?(key) }
          put_timings(o, delta_timings(rollup_timings,last_timings))
        else
          put_timings(o, rollup_timings)
        end
        last_timings = update_timings(last_timings, rollup_timings)
      end
      o.puts "Rollup processing end time:    #{rollup[:end_time]}"
      if rollup[:type] == "realtime"
        if rollup[:last_rollup_for_message]
          o.puts "Message delivered time:        #{rollup[:message_delivered_time]}"
          o.puts "Message state:                 #{rollup[:message_state]}"
          o.puts "Message delivered in:          #{rollup[:message_delivered_in]} seconds"
        end
      else
        o.puts "Message delivered time:        #{rollup[:message_delivered_time]}"
        o.puts "Message state:                 #{rollup[:message_state]}"
        o.puts "Message delivered in:          #{rollup[:message_delivered_in]} seconds"
      end
      o.puts "---"
      o.puts ""
    end
  end
end

# [----] I, [2017-02-24T05:28:23.216307 #40450:85113c]  INFO -- : MIQ(ManageIQ::Providers::Openstack::NetworkManager::MetricsCollectorWorker::Runner#get_message_via_drb) Message id: [119351], MiqWorker id: [7821], Zone: [default], Role: [ems_metrics_collector], Server: [], Ident: [openstack_network], Target id: [], Instance id: [209], Task id: [], Command: [ManageIQ::Providers::Openstack::CloudManager::Vm.perf_capture_realtime], Timeout: [600], Priority: [100], State: [dequeue], Deliver On: [], Data: [], Args: [], Dequeued in: [127.147928764] seconds

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

# [----] I, [2016-12-13T04:44:31.371524 #15020:11e598c]  INFO -- : MIQ(EmsCluster#perf_rollup) [realtime] Rollup for EmsCluster name: [MSSQL], id: [1000000000009] for time: [2016-12-13T03:08:00Z]...

realtime_perf_rollup_start_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\]
                          \ .*perf_rollup\)\ \[realtime\]\ Rollup\ for\ (?<obj_type>.+)
                          \ name:\ \[(?<obj_name>.+?)\],
                          \ id:\ \[(?<obj_id>\d+)\]
                          \ for\ time:\ \[(?<time>.+)\]\.\.\.$
                          }x

# [----] I, [2016-12-13T04:44:31.371524 #15020:11e598c]  INFO -- : MIQ(EmsCluster#perf_rollup) [realtime] Rollup for EmsCluster name: [MSSQL], id: [1000000000009] for time: [2016-12-13T03:08:00Z]...Complete - Timings: {:server_dequeue=>0.0029115676879882812, :db_find_prev_perf=>26.71201252937317, :rollup_perfs=>189.58252334594727, :db_update_perf=>156.7819893360138, :process_perfs_tag=>1.146547555923462, :process_bottleneck=>168.30106329917908, :total_time=>604.6718921661377, :purge_metrics=>12.701744079589844, :query_batch=>0.04195547103881836}

realtime_perf_rollup_complete_re = %r{
                            ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_rollup\)\ \[realtime\]\ Rollup\ for\ (?<obj_type>.+)
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\]
                            \ for\ time:\ \[(?<time>.+)\]\.\.\.Complete\ -
                            \ Timings: (?<timings>.*$)
                            }x

# [----] I, [2016-12-13T03:56:52.411916 #1439:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::HostEsx#perf_rollup) [hourly] Rollup for ManageIQ::Providers::Vmware::InfraManager::HostEsx name: [vs3.vi.grp.net - 2], id: [1000000000063] for time: [2016-12-13T01:00:00Z]...
# [----] I, [2017-01-29T12:00:45.773098 #27858:66b14c]  INFO -- : MIQ(MiqEnterprise#perf_rollup) [hourly] Rollup for MiqEnterprise name: [Enterprise], id: [1000000000001] for time: [2017-01-29T10:00:00Z]...
# [----] I, [2017-01-29T12:00:45.775557 #23910:66b14c]  INFO -- : MIQ(EmsCluster#perf_rollup) [hourly] Rollup for EmsCluster name: [Cluster AMD], id: [1000000000003] for time: [2017-01-29T10:00:00Z]...
# [----] I, [2017-01-29T04:01:18.224366 #14717:66b14c]  INFO -- : MIQ(MiqRegion#perf_rollup) [hourly] Rollup for MiqRegion name: [Region 1], id: [1000000000001] for time: [2017-01-29T02:00:00Z]...

hourly_perf_rollup_start_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\]
                          \ .*perf_rollup\)\ \[hourly\]\ Rollup\ for\ (?<obj_type>.+)
                          \ name:\ \[(?<obj_name>.+?)\],
                          \ id:\ \[(?<obj_id>\d+)\]
                          \ for\ time:\ \[(?<time>.+)\]\.\.\.$
                          }x

# [----] I, [2016-12-13T03:56:52.847443 #1439:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::HostEsx#perf_rollup) [hourly] Rollup for ManageIQ::Providers::Vmware::InfraManager::HostEsx name: [vs3.vi.grp.net - 2], id: [1000000000063] for time: [2016-12-13T01:00:00Z]...Complete - Timings: {:server_dequeue=>0.0039708614349365234, :db_find_prev_perf=>1.322969675064087, :rollup_perfs=>9.462254285812378, :db_update_perf=>3.2071571350097656, :process_perfs_tag=>70.51743388175964, :process_bottleneck=>10.498109817504883, :total_time=>97.70416975021362}
# [----] I, [2017-02-21T10:00:46.693621 #20238:33d13c]  INFO -- : MIQ(MiqRegion#perf_rollup) [hourly] Rollup for MiqRegion name: [Region 0], id: [1] for time: [2017-02-21T09:00:00Z]...Complete - Timings: {:server_dequeue=>0.0029115676879882812, :db_find_prev_perf=>26.722290515899658, :rollup_perfs=>189.67185926437378, :db_update_perf=>156.79570960998535, :process_perfs_tag=>1.1467986106872559, :process_bottleneck=>169.6373643875122, :total_time=>606.1658205986023, :purge_metrics=>12.701744079589844, :query_batch=>0.04195547103881836}
# [----] I, [2017-02-21T10:00:43.502162 #20238:33d13c]  INFO -- : MIQ(MiqEnterprise#perf_rollup) [hourly] Rollup for MiqEnterprise name: [Enterprise], id: [1] for time: [2017-02-21T09:00:00Z]...Complete - Timings: {:server_dequeue=>0.0029115676879882812, :db_find_prev_perf=>26.71651792526245, :rollup_perfs=>189.6120822429657, :db_update_perf=>156.78874564170837, :process_perfs_tag=>1.1466748714447021, :process_bottleneck=>169.6322898864746, :total_time=>606.0528314113617, :purge_metrics=>12.701744079589844, :query_batch=>0.04195547103881836}
# [----] I, [2017-02-21T10:00:42.076955 #20238:33d13c]  INFO -- : MIQ(EmsCluster#perf_rollup) [hourly] Rollup for EmsCluster name: [Default], id: [1] for time: [2017-02-21T09:00:00Z]...Complete - Timings: {:server_dequeue=>0.0029115676879882812, :db_find_prev_perf=>26.71201252937317, :rollup_perfs=>189.58252334594727, :db_update_perf=>156.7819893360138, :process_perfs_tag=>1.146547555923462, :process_bottleneck=>168.30106329917908, :total_time=>604.6718921661377, :purge_metrics=>12.701744079589844, :query_batch=>0.04195547103881836}

hourly_perf_rollup_complete_re = %r{
                            ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_rollup\)\ \[hourly\]\ Rollup\ for\ (?<obj_type>.+)
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\]
                            \ for\ time:\ \[(?<time>.+)\]\.\.\.Complete\ -
                            \ Timings: (?<timings>.*$)
                            }x

# [----] I, [2017-01-30T01:06:58.482344 #15698:66b14c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::HostEsx#perf_rollup) [daily] Rollup for ManageIQ::Providers::Vmware::InfraManager::HostEsx name: [sgop041.go.net], id: [1000000000006] for time: [2017-01-29T00:00:00Z]...
# [----] I, [2017-01-30T01:02:59.948249 #15698:66b14c]  INFO -- : MIQ(MiqEnterprise#perf_rollup) [daily] Rollup for MiqEnterprise name: [Enterprise], id: [1000000000001] for time: [2017-01-29T00:00:00Z]...
# [----] I, [2017-01-30T01:02:41.392970 #10550:66b14c]  INFO -- : MIQ(EmsCluster#perf_rollup) [daily] Rollup for EmsCluster name: [Cluster Intel], id: [1000000000004] for time: [2017-01-29T00:00:00Z]...

daily_perf_rollup_start_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\]
                          \ .*perf_rollup\)\ \[daily\]\ Rollup\ for\ (?<obj_type>.+)
                          \ name:\ \[(?<obj_name>.+?)\],
                          \ id:\ \[(?<obj_id>\d+)\]
                          \ for\ time:\ \[(?<time>.+)\]\.\.\.$
                          }x

# [----] I, [2017-01-30T01:02:41.324855 #15698:66b14c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Vm#perf_rollup) [daily] Rollup for ManageIQ::Providers::Vmware::InfraManager::Vm name: [VERD513], id: [1000000000405] for time: [2017-01-29T00:00:00Z]...Complete - Timings: {:server_dequeue=>0.0035924911499023438, :db_find_prev_perf=>3.603140115737915, :rollup_perfs=>52.784393310546875, :db_update_perf=>23.408877849578857, :process_perfs_tag=>13.1074960231781, :process_bottleneck=>3.1167943477630615, :total_time=>109.68095922470093, :purge_metrics=>2.397167205810547}
# [----] I, [2017-01-30T01:02:41.588790 #10550:66b14c]  INFO -- : MIQ(EmsCluster#perf_rollup) [daily] Rollup for EmsCluster name: [Cluster Intel], id: [1000000000004] for time: [2017-01-29T00:00:00Z]...Complete - Timings: {:server_dequeue=>0.011353015899658203, :db_find_prev_perf=>7.0114099979400635, :rollup_perfs=>161.78631830215454, :db_update_perf=>43.86720824241638, :process_perfs_tag=>127.168386220932, :process_bottleneck=>20.68321990966797, :total_time=>390.07264614105225, :purge_metrics=>3.1448822021484375}

daily_perf_rollup_complete_re = %r{
                            ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*perf_rollup\)\ \[daily\]\ Rollup\ for\ (?<obj_type>.+)
                            \ name:\ \[(?<obj_name>.+?)\],
                            \ id:\ \[(?<obj_id>\d+)\]
                            \ for\ time:\ \[(?<time>.+)\]\.\.\.Complete\ -
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
    opts.banner = "Usage: hourly_perf_rollup_timings.rb [options]"
    opts.on('-i', '--inputfile filename', 'Full file path to evm.log (if not /var/www/miq/vmdb/log/evm.log)') do |inputfile|
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
                                     :message_id  => new_message[:message_id],
                                     :status      => 'new'}
      next
    end

    realtime_started = realtime_perf_rollup_start_re.match(line)
    if realtime_started
      counter += 1
      print "Found #{counter} rollups\r"
      workers[realtime_started[:pid]] = [] unless workers.has_key?(realtime_started[:pid])
      workers[realtime_started[:pid]] << {:state    => 'realtime_rollup_started', 
                                        :type       => 'realtime',
                                        :obj_type   => realtime_started[:obj_type],
                                        :obj_name   => realtime_started[:obj_name],
                                        :obj_id     => realtime_started[:obj_id],
                                        :time       => realtime_started[:time],
                                        :start_time => realtime_started[:timestamp]}
      current = workers[realtime_started[:pid]].length - 1
      if messages.has_key?(realtime_started[:pid])
        if messages[realtime_started[:pid]][:status] == 'new'
          workers[realtime_started[:pid]][current][:first_rollup_for_message] = true
          workers[realtime_started[:pid]][current][:last_rollup_for_message] = false
          messages[realtime_started[:pid]][:status] = ''
        else
          workers[realtime_started[:pid]][current][:first_rollup_for_message] = false
        end
        workers[realtime_started[:pid]][current][:message_id] = messages[realtime_started[:pid]][:message_id]
        workers[realtime_started[:pid]][current][:message_time] = messages[realtime_started[:pid]][:timestamp]
        workers[realtime_started[:pid]][current][:message_dequeue_time] = messages[realtime_started[:pid]][:dequeued_in]
      else
        workers[realtime_started[:pid]][current][:message_time] = "No message found"
        workers[realtime_started[:pid]][current][:message_dequeue_time] = ""
      end
      next
    end

    hourly_started = hourly_perf_rollup_start_re.match(line)
    if hourly_started
      counter += 1
      print "Found #{counter} rollups\r"
      workers[hourly_started[:pid]] = [] unless workers.has_key?(hourly_started[:pid])
      workers[hourly_started[:pid]] << {:state      => 'hourly_rollup_started', 
                                        :type       => 'hourly',
                                        :obj_type   => hourly_started[:obj_type],
                                        :obj_name   => hourly_started[:obj_name],
                                        :obj_id     => hourly_started[:obj_id],
                                        :time       => hourly_started[:time],
                                        :start_time => hourly_started[:timestamp]}
      current = workers[hourly_started[:pid]].length - 1
      if messages.has_key?(hourly_started[:pid])
        workers[hourly_started[:pid]][current][:message_id] = messages[hourly_started[:pid]][:message_id]
        workers[hourly_started[:pid]][current][:message_time] = messages[hourly_started[:pid]][:timestamp]
        workers[hourly_started[:pid]][current][:message_dequeue_time] = messages[hourly_started[:pid]][:dequeued_in]
        messages.delete(hourly_started[:pid])
      else
        workers[hourly_started[:pid]][current][:message_time] = "No message found"
        workers[hourly_started[:pid]][current][:message_dequeue_time] = ""
      end
      next
    end

    daily_started = daily_perf_rollup_start_re.match(line)
    if daily_started
      counter += 1
      print "Found #{counter} rollups\r"
      workers[daily_started[:pid]] = [] unless workers.has_key?(daily_started[:pid])
      workers[daily_started[:pid]] << {:state      => 'daily_rollup_started',
                                       :type       => 'daily',
                                       :obj_type   => daily_started[:obj_type],
                                       :obj_name   => daily_started[:obj_name],
                                       :obj_id     => daily_started[:obj_id],
                                       :time       => daily_started[:time],
                                       :start_time => daily_started[:timestamp]}
      current = workers[daily_started[:pid]].length - 1
      if messages.has_key?(daily_started[:pid])
        workers[daily_started[:pid]][current][:message_id] = messages[daily_started[:pid]][:message_id]
        workers[daily_started[:pid]][current][:message_time] = messages[daily_started[:pid]][:timestamp]
        workers[daily_started[:pid]][current][:message_dequeue_time] = messages[daily_started[:pid]][:dequeued_in]
        messages.delete(daily_started[:pid])
      else
        workers[daily_started[:pid]][current][:message_time] = "No message found"
        workers[daily_started[:pid]][current][:message_dequeue_time] = ""
      end
      next
    end

    realtime_completed = realtime_perf_rollup_complete_re.match(line)
    if realtime_completed
      current = workers[realtime_completed[:pid]].length - 1 rescue next
      workers[realtime_completed[:pid]][current][:state]    = 'realtime_rollup_completed'
      workers[realtime_completed[:pid]][current][:end_time] = realtime_completed[:timestamp]
      workers[realtime_completed[:pid]][current][:timings]  = realtime_completed[:timings]
      next
    end

    hourly_completed = hourly_perf_rollup_complete_re.match(line)
    if hourly_completed
      current = workers[hourly_completed[:pid]].length - 1 rescue next
      workers[hourly_completed[:pid]][current][:state]    = 'hourly_rollup_completed'
      workers[hourly_completed[:pid]][current][:end_time] = hourly_completed[:timestamp]
      workers[hourly_completed[:pid]][current][:timings]  = hourly_completed[:timings]
      next
    end

    daily_completed = daily_perf_rollup_complete_re.match(line)
    if daily_completed
      current = workers[daily_completed[:pid]].length - 1 rescue next
      workers[daily_completed[:pid]][current][:state]    = 'daily_rollup_completed'
      workers[daily_completed[:pid]][current][:end_time] = daily_completed[:timestamp]
      workers[daily_completed[:pid]][current][:timings]  = daily_completed[:timings]
      next
    end

    message_delivered = message_delivered_re.match(line)
    if message_delivered
      current = workers[message_delivered[:pid]].length - 1 rescue next
      if message_delivered[:message_id] == workers[message_delivered[:pid]][current][:message_id]
        workers[message_delivered[:pid]][current][:message_delivered_time] = message_delivered[:timestamp]
        workers[message_delivered[:pid]][current][:message_state]          = message_delivered[:state]
        workers[message_delivered[:pid]][current][:message_delivered_in]   = message_delivered[:delivered_in]
        workers[message_delivered[:pid]][current][:last_rollup_for_message] = true
      end
      next
    end
  end
  stats(workers, options)

rescue => err
  puts "[#{err}]\n#{err.backtrace.join("\n")}"
  # puts "#{perf_rollup_workers.inspect}"
  exit!
end




