package rmq

var redisScripts = map[string]string{
	"publish": `
		local call = redis.call

		local readyQueue = KEYS[1]
		local priorityQueue = KEYS[2]
		local value = ARGV[1]
		local jobPriority = tonumber(ARGV[2])

		local jobId = call("INCR", "increasing_id")
		call('set', jobId .. "_value", value);
		call('set', jobId .. "_priority", jobPriority);

		if jobPriority == 0 then
			call('LPUSH', readyQueue, jobId);
		else
			call("ZADD", priorityQueue, jobPriority,  jobId)
		end
	`,

	"consume": `
		local call = redis.call

		local readyQueue = KEYS[1]
		local unackedQueue = KEYS[2]
		local priorityQueue = KEYS[3]

		local jobId

		local results = call("ZPopMax", priorityQueue)
		local length = #results

		if length == 2 then
			jobId = results[1]
			call("LPush", unackedQueue, jobId)
		else
			jobId = call("RPopLPush", readyQueue, unackedQueue)
		end

		return jobId
	`,

	"ack": `
		local call = redis.call

		local unackedQueue = KEYS[1]
		local jobId = tonumber(ARGV[1])

		local count = call("LREM", unackedQueue, 1, jobId)
		call("DEL", jobId .. "_value")
		call("DEL", jobId .. "_priority")

		return count
	`,

	"move": `
		local call = redis.call

		local sourceQueue = KEYS[1]
		local destinationQueue = KEYS[2]
		local priorityQueueSource = KEYS[3]
		local priorityQueueDestination = KEYS[4]

		local jobId = tonumber(ARGV[1])

		call("LREM", sourceQueue, 1, jobId)

		if priorityQueueSource then
			call("ZREM", priorityQueueSource, jobId)
		end

		if priorityQueueDestination then
			local jobPriority = call("GET", jobId .. "_priority")
			if jobPriority == 0 then
				call('LPUSH', destinationQueue, jobId);
			else
				call("ZADD", priorityQueueDestination, jobPriority,  jobId)
			end
		else
			call("LPUSH", destinationQueue, jobId)
		end
	`,
}
