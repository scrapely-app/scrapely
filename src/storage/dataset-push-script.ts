import type Redis from 'ioredis';

/**
 * Atomic dataset push: allocates consecutive item slots starting at the current
 * itemCount, writes the items, and updates itemCount/storageBytes/modifiedAt in
 * a single script.
 *
 * Without this, pushItems did a non-atomic `HGET itemCount` followed by HSETs of
 * `count..count+n` in a pipeline, so two concurrent pushes could read the same
 * count and write overlapping slots — silently overwriting items and skewing
 * itemCount away from HLEN (which also broke the dense-index pagination
 * assumption used by both this client and the server route).
 */
export const PUSH_ITEMS_LUA = `
local infoKey = KEYS[1]
local itemsKey = KEYS[2]
local now = ARGV[1]
local bytes = tonumber(ARGV[2])
local count = tonumber(redis.call('HGET', infoKey, 'itemCount') or '0')
for i = 3, #ARGV do
  redis.call('HSET', itemsKey, tostring(count), ARGV[i])
  count = count + 1
end
redis.call('HSET', infoKey, 'itemCount', tostring(count))
redis.call('HINCRBY', infoKey, 'storageBytes', bytes)
redis.call('HSET', infoKey, 'modifiedAt', now)
return count
`;

/**
 * Pushes serialized items into the dataset hash, allocating dense consecutive
 * slots atomically. Returns the new itemCount.
 */
export async function pushItemsAtomic(
    redis: Redis,
    infoKey: string,
    itemsKey: string,
    serializedItems: string[],
    totalBytes: number,
): Promise<number> {
    const result = await redis.eval(
        PUSH_ITEMS_LUA,
        2,
        infoKey,
        itemsKey,
        new Date().toISOString(),
        String(totalBytes),
        ...serializedItems,
    );
    return typeof result === 'number' ? result : parseInt(String(result), 10);
}
