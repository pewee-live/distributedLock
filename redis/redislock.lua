--[[  
	 成功返回1,不成功返回0,非法操作返回3
     KEYS[1]:操作为lock和unlock,KEYS[1]赋值为lock的Key，ARGV[1]赋值为requestId  
--]] 
local key = KEYS[2] .. ARGV[1]
if KEYS[1] == 'lock' then
	if redis.call("EXISTS", key) == 1 then --[[有的话肯定不为0]]--
		local temp = tonumber(redis.call('get', key)) + 1 --[[获取成功+1]]--
		redis.call("set", key , temp)
		return 1;
	else
		redis.call('set', key, 1)
		return 1;
	end
end
if KEYS[1] == 'unlock' then
	if redis.call("EXISTS", key) == 1 then 
		if tonumber(redis.call('get', key)) > 1 then 
			local temp = tonumber(redis.call('get', key)) - 1
			redis.call('set', key , temp)
			return 1
		else 
			return redis.call('del', key)
		end 
	else
		 return 3
	end
end