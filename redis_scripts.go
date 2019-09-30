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
}
