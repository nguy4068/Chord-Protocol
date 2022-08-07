#!/bin/bash
count=0
USERID=`whoami`
while read p; do
   varname=`echo $p | cut  -f 1 -d' '`
   varval=`echo $p | cut --complement -f 1 -d' '`
   setenvstring="$setenvstring setenv $varname $varval &&"
   if [ $varname == "PROJ_PATH" ] ; then
      PROJ_PATH=`echo $p | cut --complement -f 1 -d' '`
   elif [ $varname == "SLEEP" ] ; then
      SLEEP_TIME=$varval
   fi
   count=`echo "scale=0;$count + 1" | bc`
done < env.txt
count=0
while read p; 
do
  echo "read node $p"
  if [ $count == 0 ] ; then
    SUPERNODE=`echo $p`
    echo "supernode $p" > supernode_config.txt
    HOST=`echo $p | cut  -f 1 -d' '`
    SUPERNODE_COMMAND=`echo "ant supernode"`
    CA=`echo "$setenvstring  cd $PROJ_PATH &&  nohup $SUPERNODE_COMMAND & "`
    COMMAND_FILE=`echo "supernode_command.sh"`
    echo "ssh -t -n -f $USERID@$HOST \"csh -c '$CA'\" > /dev/null"  > $COMMAND_FILE
  else
     CONFIG=`echo "config$count.txt"`
     HOST=`echo $p | cut  -f 1 -d' '`
     echo "$p" > $CONFIG
     NODE_COMMAND=`echo "ant node -Dnodeconfig=$CONFIG -Doutput=output$count.log"`
     CA=`echo "$setenvstring  cd $PROJ_PATH &&  nohup $NODE_COMMAND & "`
     COMMAND_FILE=`echo "node_command$count.sh"`
     echo "ssh -t -n -f $USERID@$HOST \"csh -c '$CA'\" > /dev/null"  > $COMMAND_FILE
  fi
  count=`echo "scale=0;$count + 1" | bc`
done < nodes_config.txt
echo "csh -c '$setenvstring ant client'" > client.sh
source supernode_command.sh
echo "Sleeping while waiting supernode to boot up"
sleep 4
echo "Starting to init node"

a=1
while [ $a -lt $count ]
do
   echo "fire node_command$a.sh"
   command_file=`echo "node_command$a.sh"`
   source $command_file
   a=`expr $a + 1`
   echo "Sleep $SLEEP_TIME seconds"
   sleep $SLEEP_TIME
done

echo "Sleeping while waiting for nodes to finish joining Chord system"
sleep 5
echo "Nodes finish joining chord, now you can go ahead and run the client"

########################
