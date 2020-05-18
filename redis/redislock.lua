--[[  
	定义本地函数
--]] 
local function getReqId(redisString) 
	local splitstr = '_';
	local indexS,indexE = string.find(redisString,splitstr)
	local result = string.sub(redisString,1,indexS - 1)
	return result
end 

local function getNum(redisString) 
	local splitstr = '_';
	local indexS,indexE = string.find(redisString,splitstr)
	local result = string.sub(redisString,indexE + 1,-1)
	return result
end

local function removeHashTag(key) 
	local splitstr = '}';
	local indexS,indexE = string.find(key,splitstr)
	local result = string.sub(key,indexE + 1,-1)
	return result
end
--[[  
	 成功返回1,不成功返回0,非法操作返回3
     KEYS[1]:操作为lock和unlock,KEYS[2]赋值为lock的Key，ARGV[1]赋值为requestId  
--]] 
local key = KEYS[2]
local reqId = ARGV[1]
local calcLock = removeHashTag(KEYS[1])
if calcLock == 'lock' then
	if redis.call("EXISTS", key) == 1 then --[[有的话肯定不为0]]--
		local redisStr = redis.call('get', key)
		if getReqId(redisStr) == reqId then
			local redisnum = getNum(redisStr)
			local redistemp = tonumber(redisnum) + 1 --[[获取成功+1]]--
			redis.call('setex', key , 600 , reqId .. '_' .. string.format('%d',redistemp)) --[[SETEX key seconds value]]--
			return 1
		else 
			return 0
		end
	else --[[没有设置进去]]--
		redis.call("setex", key , 600 , reqId .. '_' .. string.format('%d',1)) --[[SETEX key seconds value]]--
		return 1;
	end
elseif calcLock == 'unlock' then
	if redis.call("EXISTS", key) == 1 then 
		local redisStr = redis.call('get', key)
		if getReqId(redisStr) == reqId then
			local redisnum = getNum(redisStr)
			if tonumber(redisnum) > 1 then 
				local redistemp = tonumber(redisnum) - 1
				redis.call('setex', key , 600 , reqId .. '_' .. string.format('%d',redistemp))
				return 1
			else 
				return redis.call('del', key)
			end 
		else 
			return 0
		end
	else
		 return 3
	end
else 
	return calcLock
end

