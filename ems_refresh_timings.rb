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
      next if /ems_refresh/.match(metric)
      format = /^num_.*/.match(metric) ? "%-36s %5.6f" : "%-36s %5.6f seconds"
      o.puts "  #{sprintf(format, "#{metric}:", timings[metric])}" unless timings[metric].zero?
    end
    # print ems_refresh last in each section
    if timings.has_key?(:ems_refresh)
      o.puts "  #{sprintf("%-36s %5.6f seconds", "ems_refresh:", timings[:ems_refresh])}" unless timings[:ems_refresh].zero?
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

  refresh_counters = [:ems_refresh,:collect_inventory_for_targets,:parse_targeted_inventory,:save_inventory,
                      :parse_legacy_inventory,:get_ems_data,:get_vc_data,:get_vc_data_ems_customization_spec,
                      :filter_vc_data,:get_vc_data_host_scsi,:parse_vc_data,:db_save_inventory,:fetch_host_data,
                      :fetch_vm_data,:fetch_all,:parse_inventory,:total_time]
  non_refresh_counters = [:server_dequeue,:heartbeat,:server_monitor,:log_active_servers,:worker_monitor,:worker_dequeue]


  if options[:outputfile]
    o = File.open(options[:outputfile],'w')
  else
    o = $stdout.dup
  end

  workers.each do |pid, messages|
    last_timings = {}
    messages.each do |refresh|
      o.puts "---"
      o.puts "Worker PID:             #{pid}"
      o.puts "Message ID:             #{refresh[:message_id]}"
      o.puts "Message fetch time:     #{refresh[:message_time]}"
      o.puts "Message time in queue:  #{refresh[:message_dequeue_time]} seconds"
      o.puts "Provider:               #{refresh[:provider]}"
      o.puts "EMS Name:               #{refresh[:ems_name]}"
      o.puts "Refresh type:           #{refresh[:type]}"
      if refresh[:type] == 'targeted'
        targets = ""
        refresh[:targets].keys.each do |target|
          targets += ", " unless targets == ""
          targets += "#{target}: #{refresh[:targets][target].length}"
        end
        o.puts "Refresh targets:        #{targets}"
      end
      o.puts "Refresh start time:     #{refresh[:start_time]}"
      o.puts "Refresh timings:"
      timings = eval(refresh[:timings]) if @timings_re.match(refresh[:timings])
      unless timings.nil?
        if (timings.keys & (non_refresh_counters)).any?
          # o.puts "*** Debug - BZ1424716 ***"
          # Need to delete the erroneous counters then subtract previous counters from the remainder (https://bugzilla.redhat.com/show_bug.cgi?id=1424716)
          timings.delete_if { |key, _| !refresh_counters.include?(key) }
          put_timings(o, delta_timings(timings,last_timings))
        else
          put_timings(o, timings)
        end
        last_timings = update_timings(last_timings, timings)
      end
      o.puts "Refresh end time:       #{refresh[:end_time]}"
      o.puts "Message delivered time: #{refresh[:message_delivered_time]}"
      o.puts "Message state:          #{refresh[:message_state]}"
      o.puts "Message delivered in:   #{refresh[:message_delivered_in]} seconds"
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

# [----] I, [2017-01-23T11:39:28.842576 #40611:4ad134]  INFO -- : MIQ(ManageIQ::Providers::Amazon::CloudManager::Refresher#refresh) EMS: [ec2], id: [1] Refreshing targets for EMS...
# CFME 5.8
# [----] I, [2017-04-25T08:49:41.359916 #26815:106312c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresh::Strategies::Api3#refresh) EMS: [RHEV], id: [1] Refreshing targets for EMS...

ems_refresh_start_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                          \ \#(?<pid>\h+):\h+\]
                          \ .*MIQ\(ManageIQ::Providers::(?<provider>.+)::Refresh(?:.+)\#refresh\)
                          \ EMS:\ \[(?<ems_name>.+)\],
                          \ id:\ \[(?<ems_id>\d+)\]
                          \ Refreshing\ targets\ for\ EMS\.\.\.$
                          }x

# [----] I, [2016-12-13T23:33:50.905887 #17949:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Refresher#refresh) EMS: [vCenter6], id: [1000000000002]   ManageIQ::Providers::Vmware::InfraManager [vCenter6] id [1000000000002]
# [----] I, [2017-02-17T05:53:09.941213 #24912:49514c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresher#refresh) EMS: [RHEV], id: [1]   ManageIQ::Providers::Redhat::InfraManager [RHEV] id [1]
# [----] I, [2017-01-23T09:59:27.041079 #36769:4ad134]  INFO -- : MIQ(ManageIQ::Providers::Amazon::CloudManager::Refresher#refresh) EMS: [ec2], id: [1]   ManageIQ::Providers::Amazon::CloudManager [ec2] id [1]
# [----] I, [2017-01-23T10:31:34.701712 #16603:4ad134]  INFO -- : MIQ(ManageIQ::Providers::Amazon::NetworkManager::Refresher#refresh) EMS: [ec2 Network Manager], id: [2]   ManageIQ::Providers::Amazon::NetworkManager [ec2 Network Manager] id [2]
# CFME 5.8
# [----] I, [2017-04-25T08:47:04.011432 #26815:106312c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresh::Strategies::Api3#refresh) EMS: [RHEV], id: [1]   ManageIQ::Providers::Redhat::InfraManager [RHEV] id [1]

ems_refresh_full_re = %r{
                         ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                         \ \#(?<pid>\h+):\h+\]
                         \ .*MIQ\(ManageIQ::Providers::(?<provider>.+)::Refresh(?:.+)\#refresh\)
                         \ EMS:\ \[(?<ems_name>.+)\],
                         \ id:\ \[(?<ems_id>\d+)\]
                         \s+ManageIQ::Providers::.*Manager[^:]
                        }x

# [----] I, [2016-12-13T05:10:33.948418 #18844:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Refresher#refresh) EMS: [vCenter6], id: [1000000000002]   ManageIQ::Providers::Vmware::InfraManager::Vm [S0188VSS008] id [1000000001241]
# [----] I, [2016-12-14T01:46:59.361032 #15760:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Refresher#refresh) EMS: [vCenter6], id: [1000000000002]   ManageIQ::Providers::Vmware::InfraManager::HostEsx [vs4-01-07.vi.bit63.net - 2] id [1000000000038]
# [----] I, [2017-02-17T09:42:00.540557 #29182:49514c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresher#refresh) EMS: [RHEV], id: [1]   ManageIQ::Providers::Redhat::InfraManager::Vm [jst-db01] id [10]
# CFME 5.8
# [----] I, [2017-04-25T10:22:20.586073 #26815:106312c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresh::Strategies::Api3#refresh) EMS: [RHEV], id: [1]   ManageIQ::Providers::Redhat::InfraManager::Vm [jst-mid01] id [14]

ems_refresh_targeted_re = %r{
                          ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*MIQ\(ManageIQ::Providers::(?<provider>.+)::Refresh(?:.+)\#refresh\)
                            \ EMS:\ \[(?<ems_name>.+)\],
                            \ id:\ \[(?<ems_id>\d+)\]
                            \s+ManageIQ::Providers::.*Manager::(?<target_type>\w+)\ \[(?<target_name>.+?)\](:?\.\.\.)*$
                            }x

# [----] I, [2017-01-23T11:40:07.609550 #40611:4ad134]  INFO -- : MIQ(ManageIQ::Providers::Amazon::CloudManager::Refresher#refresh) EMS: [ec2], id: [1] Refreshing targets for EMS...Complete - Timings {:server_dequeue=>0.0038323402404785156, :collect_inventory_for_targets=>5.0067901611328125e-06, :parse_legacy_inventory=>18.24713921546936, :parse_targeted_inventory=>18.247156858444214, :save_inventory=>20.513510942459106, :ems_refresh=>38.76088333129883}
# [----] I, [2016-12-13T03:49:33.920716 #14142:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Refresher#refresh) EMS: [vCenter6], id: [1000000000002] Refreshing targets for EMS...Complete - Timings {:server_dequeue=>0.005602121353149414, :get_ems_data=>98.27816534042358, :get_vc_data=>1034.553739786148, :filter_vc_data=>0.4792180061340332, :get_vc_data_host_scsi=>385.1608579158783, :collect_inventory_for_targets=>1520.600543498993, :parse_vc_data=>6.759590148925781, :parse_targeted_inventory=>6.783946990966797, :db_save_inventory=>771.571896314621, :save_inventory=>771.5742604732513, :ems_refresh=>2298.999431371689}
# [----] I, [2016-12-13T17:37:14.602687 #20979:11e598c]  INFO -- : MIQ(ManageIQ::Providers::Vmware::InfraManager::Refresher#refresh) EMS: [vCenter6], id: [1000000000002] Refreshing targets for EMS...Complete - Timings {:server_dequeue=>0.008832931518554688, :get_ems_data=>42.64505863189697, :get_vc_data=>410.24549293518066, :filter_vc_data=>0.28925633430480957, :get_vc_data_host_scsi=>700.0426359176636, :collect_inventory_for_targets=>1161.7626280784607, :parse_vc_data=>5.340915679931641, :parse_targeted_inventory=>5.3509438037872314, :db_save_inventory=>1082.315097808838, :save_inventory=>1082.3164196014404, :ems_refresh=>2249.452482700348, :get_vc_data_ems_customization_specs=>7.261430978775024}
# [----] I, [2017-01-29T12:28:28.467210 #28489:fd7140]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresher#refresh) EMS: [RHEV], id: [1] Refreshing targets for EMS...Complete - Timings {:server_dequeue=>0.0031862258911132812, :fetch_all=>3.3063478469848633, :collect_inventory_for_targets=>6.957422971725464, :parse_inventory=>0.01478123664855957, :parse_targeted_inventory=>0.015095710754394531, :save_inventory=>10.616236209869385, :ems_refresh=>17.591099739074707, :fetch_vm_data=>1.4342906475067139}
# [----] I, [2017-01-30T10:46:24.840095 #2179:fd7140]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresher#refresh) EMS: [RHEV], id: [1] Refreshing targets for EMS...Complete - Timings {:server_dequeue=>0.0062427520751953125, :fetch_all=>3.247378349304199, :collect_inventory_for_targets=>4.212141275405884, :parse_inventory=>0.02175760269165039, :parse_targeted_inventory=>0.021877288818359375, :save_inventory=>10.164296627044678, :ems_refresh=>14.399087905883789}
# CFME 5.8
# [----] I, [2017-04-25T08:42:24.148037 #26815:106312c]  INFO -- : MIQ(ManageIQ::Providers::Redhat::InfraManager::Refresh::Strategies::Api3#refresh) EMS: [RHEV], id: [1] Refreshing targets for EMS...Complete - Timings {:fetch_all=>9.840977430343628, :collect_inventory_for_targets=>12.333699941635132, :parse_inventory=>0.07196855545043945, :parse_targeted_inventory=>0.07205867767333984, :save_inventory=>28.14843249320984, :ems_refresh=>40.55474328994751}

ems_refresh_complete_re = %r{
                            ----\]\ I,\ \[(?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})
                            \ \#(?<pid>\h+):\h+\]
                            \ .*MIQ\(ManageIQ::Providers::(?<provider>.+)::Refresh(?:.+)\#refresh\)
                            \ EMS:\ \[(?<ems_name>.+)\],
                            \ id:\ \[(?<ems_id>\d+)\]
                            \ Refreshing\ targets\ for\ EMS\.\.\.Complete\ -
                            \ Timings (?<timings>.*$)
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

#all_timings = {
#  :server_dequeue => 0,
#  # Common (mixin) timings
#  :ems_refresh                        => 0,
#  :collect_inventory_for_targets      => 0,
#  :parse_targeted_inventory           => 0,
#  :save_inventory                     => 0,
#  :parse_legacy_inventory             => 0,
#  # VMware-specific timings
#  :get_ems_data                       => 0,
#  :get_vc_data                        => 0,
#  :get_vc_data_ems_customization_spec => 0,
#  :filter_vc_data                     => 0,
#  :get_vc_data_host_scsi              => 0,
#  :parse_vc_data                      => 0,
#  :db_save_inventory                  => 0,
#  # RHEV-specific timings
#  :fetch_host_data                    => 0,
#  :fetch_vm_data                      => 0,
#  :fetch_all                          => 0,
#  :parse_inventory                    => 0
#}#

#min_timings = all_timings
#max_timings = all_timings

begin
  options = {:inputfile => nil, :outputfile => nil}
  
  parser = OptionParser.new do|opts|
    opts.banner = "Usage: ems_refresh_timings.rb [options]"
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

    started = ems_refresh_start_re.match(line)
    if started
      counter += 1
      print "Found #{counter} refreshes\r"
      workers[started[:pid]] = [] unless workers.has_key?(started[:pid])
      workers[started[:pid]] << {:state      => 'started', 
                                            :provider   => started[:provider], 
                                            :ems_name   => started[:ems_name],
                                            :start_time => started[:timestamp]}
      current = workers[started[:pid]].length - 1
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

    full = ems_refresh_full_re.match(line)
    if full
      current = workers[full[:pid]].length - 1
      workers[full[:pid]][current][:type] = 'full'
      next
    end
    
    targeted = ems_refresh_targeted_re.match(line)
    if targeted
      current = workers[targeted[:pid]].length - 1 rescue next
      workers[targeted[:pid]][current][:type]    = 'targeted' unless workers[targeted[:pid]][current].has_key?(:type)
      workers[targeted[:pid]][current][:targets] = {} unless workers[targeted[:pid]][current].has_key?(:targets)
      workers[targeted[:pid]][current][:targets][targeted[:target_type]] = [] unless workers[targeted[:pid]][current][:targets][targeted[:target_type]].class.to_s == "Array"
      workers[targeted[:pid]][current][:targets][targeted[:target_type]] << targeted[:target_name]
      next
    end

    completed = ems_refresh_complete_re.match(line)
    if completed
      current = workers[completed[:pid]].length - 1 rescue next
      workers[completed[:pid]][current][:state]    = 'completed'
      workers[completed[:pid]][current][:end_time] = completed[:timestamp]
      workers[completed[:pid]][current][:timings]  = completed[:timings]
      next
    end

    message_delivered = message_delivered_re.match(line)
    if message_delivered
      current = workers[message_delivered[:pid]].length - 1 rescue next
      if message_delivered[:message_id] == workers[message_delivered[:pid]][current][:message_id]
        workers[message_delivered[:pid]][current][:message_delivered_time] = message_delivered[:timestamp]
        workers[message_delivered[:pid]][current][:message_state]          = message_delivered[:state]
        workers[message_delivered[:pid]][current][:message_delivered_in]   = message_delivered[:delivered_in]
      end
      next
    end
  end
  stats(workers, options)

rescue => err
  puts "[#{err}]\n#{err.backtrace.join("\n")}"
  exit!
end




