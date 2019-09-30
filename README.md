#### forked from https://github.com/adjust/rmq

#### added:
-tasks can be assigned a priority number (to be processed sooner from "ready" queue)
<br/>-queues now hold ids of tasks (integers) instead of payloads
<br/>-introduced redis lua scripts for isolation
<br/>-option to run queue cleaner automatically in background every minute, will be run once per minute no matter how many connections (using distributed lock)
<br/>-adjusted tests to fit the new functionality, all passing for old functionality
<br/>-adjusted examples to new functionality

#### todo:
-example for priority
<br/>-test suite for priority
<br/>-list items of each queue
<br/>-forwarding of delivery to another queue (alternative to push queues)
<br/>-use HMSET to store value & metadata
<br/>-calculate the stats inside single redis script to get precise counts
<br/>-delayed tasks
<br/>-recovery in case redis script fails in the middle (rare case)
