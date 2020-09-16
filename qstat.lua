local fiber = require("fiber")
local config = require('config')
local log = require('log')
local queue = require 'queue'
local graphite = require 'net.graphite'

local M = {}

local stat_job_name = 'stat_job'
local stat = graphite(config.get('app.metrics'))

local fiber_object 

function M.check_format()
	for _, tube_rc in box.space._queue:pairs() do
		local tube_name = tube_rc[1]
		local format = box.space[tube_name]:format() 

		local len = #format
		if len < 8 then
			log.error("Fields count less 8")
			return false
		end

		if format[1]['name'] ~= 'task_id' or 
		format[2]['name'] ~= 'status' or 
		format[6]['name'] ~= 'pri' then
			log.error("Unexpected fields name")
			return false
		end
	end

	return true
end

function M.safe_send_stat()
	local iteration_count = config.get('app.iteration_count') or 30000
	local time_to_sleep = config.get('app.metrics_time_to_sleep') or 30

	log.info("start statistics timeout: %d", time_to_sleep)
	if not M.check_format() then
		log.error("Invalid queue format")
		return
	end

	while true do
		local ok, err = pcall(M.send_stat, iteration_count)
		if not ok then
			log.error("send_stat return err: %s", err)
		end


		fiber.sleep(time_to_sleep)
	end
end

function M.send_stat(iteration_count)
	local tasks_statistics = {}
	-- по всем очередям
	for _, tube_rc in box.space._queue:pairs() do
		local tube_name = tube_rc[1]

		--local all_count = box.space[tube_name]:len()
		local last_id = 0
		while true do
			local condition = 'GT'
			if last_id == 0 then
				condition = 'GE'
			end

			local tuples = box.space[tube_name]:select({last_id}, {iterator = condition, limit = iteration_count })
			for k, t in ipairs(tuples) do
				local status = t[2]
				local priority = t[6]

				-- сохраняем в виде:
				-- ['r'][25] = 208
				-- ['~'][30] = 4208
				local status_name = tasks_statistics[status]
				if status_name == nil then
					tasks_statistics[status] = {}
					tasks_statistics[status][priority] = 1
				else
					local count = tasks_statistics[status][priority]
					if count == nil then
						tasks_statistics[status][priority] = 1
					else
						tasks_statistics[status][priority] = count + 1
					end
				end
			end

			local len = #tuples
			if len > 0 then
				last_id = tuples[len][1]
			else
				break
			end

			fiber.yield()
		end

		local queue_statuses = {
			['r'] = 'ready',
			['t'] = 'taken',
			['~'] = 'delayed',
			['!'] = 'buried',
			['-'] = 'removed',
		}

		for status, v in pairs(tasks_statistics) do
			for priority, count in pairs(v) do
				local name = queue_statuses[status] or 'unknown'
				local key = string.format('tasks.%s.%d', name, priority)
				stat:send(key, count)
				log.info('%s %s', key, count)
			end
		end

    end
end

function M.start()
	fiber_object = fiber.create(function()
		fiber.name(stat_job_name)
		M.safe_send_stat()
	end)
end

function M.stop()
	if fiber_object ~= nil then
		fiber_object.cancel()
	end
end

return M