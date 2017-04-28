BEGIN {   FS=" "
         OFS=","
         CONVFMT="%.3f"
         OFMT="%.3f"
       print "date-time,pid-tid,messageid,Ident,queued time, element count,PRovider cnt,Host cnt,VM cnt,Template cnt,duration,completion code"
       }
/EmsRefresh\.refresh/ { if ( $0 ~ /(\.|\#)get/) {
                                            # print
                                            completion_code =""
                                            date_string = substr($3,2,10)
                                            time_string = substr($3,13)
                                            pidtid = substr($4,2,length($4)-2)
                                            arg_string = substr($0 , index($0, "Args: ") + 6)  
#                                            element_count = gsub(/\,/,",",arg_string) / 2 
                                            element_count = 0
                                            vm_args = arg_string
                                            vm_count = gsub(/\"Vm/,"\"Vm",vm_args)
                                            vm_count = gsub(/\:\:Vm\"/,"\:\:Vm\"",vm_args) + vm_count
                                            element_count = element_count + vm_count
                                            host_args = arg_string
                                            host_count = gsub(/"Host/,"\"Host",host_args)
                                            host_count = gsub(/\:\:Host/,"\:\:Host",host_args) + host_count
                                            element_count = element_count + host_count
                                            ems_args = arg_string
                                            ems_count = gsub(/\"(Ems|Ext)/,"\"Exx",ems_args)
                                            ems_count = gsub(/\:\:InfraManager\"/,"\:\:InfraManager\"",ems_args) + ems_count
                                            element_count = element_count + ems_count
#                                            print ems_count
                                            template_args = arg_string
                                            template_count = gsub(/"Template/,"\"Template",template_args)
                                            template_count = gsub(/\:\:Template/,"\:\:Template",template_args) + template_count
                                            element_count = element_count + template_count
#                                            print $(NF-1)
#                                            dequeue_time = "\""substr($(NF-1),2,length($(NF-1)-2))"\""
#                                            print dequeue_time
                                            dequeue_time = $(NF-1)
                                            
                                            ident_id = substr($0,index($0,"Ident: ") + length("Ident: ") )
                                            ident_id = substr(ident_id,1,index(ident_id,",") - 1 )
                                            
                                            message_id = substr($11,2,length($11)-3)
#                                            print message_id, dequeue_time, "element count is " element_count,ems_count,host_count,vm_count,template_count
                                            msgid_array[$11] = date_string" "time_string","pidtid","message_id","ident_id","dequeue_time","element_count","ems_count"," host_count","vm_count","template_count","

                                            }
                       }
/MIQ\(MiqQueue.delivered\)/ {  array_key = $11
                                completion_code = $(NF-4) 
# gather completion code                                
                                if ($8 ~ "Q-task_id") {array_key = $12 }


                                if ( array_key in msgid_array) { msgid_array[array_key] = msgid_array[array_key] $(NF-1)","completion_code
                                                          gsub(/\[/,"",msgid_array[array_key])
                                                          gsub(/\]/,"",msgid_array[array_key])
                                                          print msgid_array[array_key] 
                                                         delete msgid_array[array_key]
 
                                                         }

                              }                                                                                   
       
END { #print "printing left over messages" 
      for ( msgid in msgid_array)  { print msgid_array[msgid]  "no record of completion"}
      delete msgid_array                                                                                                                  
     }       
