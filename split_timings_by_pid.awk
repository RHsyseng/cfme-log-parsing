BEGIN {
  in_block = 0
}

/Worker PID:/ {outfile = sprintf("%s_pid_%s", FILENAME, $3)
                print "---" >> outfile
                in_block = 1
              }

{if (in_block == 1) 
    print $0 >> outfile
}

/Message delivered in:/ {
                         print "---" >> outfile
                         print "" >> outfile
                         in_block = 0
                        }

END {}

