#!/bin/bash

HDPPATH="/home/yavcular/hadoop-0.21.0"

"$HDPPATH"/bin/hadoop fs -rmr /user/yasemin/piccolo/output
# "$HDPPATH"/bin/hadoop jar "$HDPPATH"/hadoop-piccolo.jar edu/nyu/cs/piccolo/examples/WordCount /user/yasemin/webgraph/output/foucusedcrawl/raw-to-link-graph/part-r-00000 /user/yasemin/piccolo/output
"$HDPPATH"/bin/hadoop jar "$HDPPATH"/hadoop-piccolo-to-run.jar edu/nyu/cs/piccolo/examples/WordCount /user/yasemin/piccolo/input/  /user/yasemin/piccolo/output

