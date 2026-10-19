import type Redis from 'ioredis';

/**
 * Atomic list-and-lock. Returns a two-element Lua table:
 *   { lockedIds, ZCARD(lockedKey) }
 * which ioredis delivers as [string[], number]. The ZCARD is taken after the
 * script's expired-lock purge and after this call's own locks are added, so it
 * equals the number of active locks held by ANY client — exactly what the
 * `queueHasLockedRequests` flag means ("locked by any client, either
 * the one calling the endpoint or a different one"). Callers get the flag from
 * this same atomic reply instead of re-reading the whole locked + pending sets.
 */
export const LIST_AND_LOCK_HEAD_LUA = `
local pendingKey = KEYS[1]
local lockedKey = KEYS[2]
local lockOwnersKey = KEYS[3]
local requestsKey = KEYS[4]
local now = tonumber(ARGV[1])
local lockUntil = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local ownerValue = ARGV[4]
local scanLimit = tonumber(ARGV[5])

redis.call('ZREMRANGEBYSCORE', lockedKey, '-inf', now)

local pendingIds = redis.call('ZRANGE', pendingKey, 0, scanLimit - 1)
local lockedIds = {}

for _, id in ipairs(pendingIds) do
  if #lockedIds >= limit then break end
  local lockScore = redis.call('ZSCORE', lockedKey, id)
  if not lockScore or tonumber(lockScore) <= now then
    if redis.call('HEXISTS', requestsKey, id) == 1 then
      redis.call('ZADD', lockedKey, lockUntil, id)
      redis.call('HSET', lockOwnersKey, id, ownerValue)
      table.insert(lockedIds, id)
    end
  end
end

-- Everything left in lockedKey is an active lock (score > now): the purge
-- above removed the expired ones and we just added ours. An O(1) ZCARD here
-- replaces the callers' old tail of ZRANGEBYSCORE(locked) + full
-- ZRANGE(pending, 0, -1) scans on every head fetch.
return { lockedIds, redis.call('ZCARD', lockedKey) }
`;

export interface ListAndLockHeadKeys {
    pendingKey: string;
    lockedKey: string;
    lockOwnersKey: string;
    requestsKey: string;
}

export interface ListAndLockHeadParams {
    now: number;
    lockUntil: number;
    limit: number;
    ownerValue: string;
    /** How many pending IDs to scan (should be >= limit). */
    scanLimit?: number;
}

export interface ListAndLockHeadAtomicResult {
    /** IDs locked by this call, in queue-head order. */
    lockedIds: string[];
    /**
     * Active locks (lockUntil > now) immediately after this atomic operation,
     * including the locks this call just created (any client's locks count).
     */
    activeLockCount: number;
}

export async function listAndLockHeadAtomic(
    redis: Redis,
    keys: ListAndLockHeadKeys,
    params: ListAndLockHeadParams,
): Promise<ListAndLockHeadAtomicResult> {
    const scanLimit = params.scanLimit ?? Math.max(params.limit * 4, 100);
    const result = await redis.eval(
        LIST_AND_LOCK_HEAD_LUA,
        4,
        keys.pendingKey,
        keys.lockedKey,
        keys.lockOwnersKey,
        keys.requestsKey,
        String(params.now),
        String(params.lockUntil),
        String(params.limit),
        params.ownerValue,
        String(scanLimit),
    );

    // Expected reply shape: [string[], number]. Tolerate degraded replies the
    // same way the old string[] version did (null / JSON-stringified table).
    if (!Array.isArray(result) || result.length < 2) {
        return { lockedIds: [], activeLockCount: 0 };
    }

    let lockedIds = result[0];
    if (typeof lockedIds === 'string') {
        try {
            lockedIds = JSON.parse(lockedIds);
        } catch {
            lockedIds = [];
        }
    }
    if (!Array.isArray(lockedIds)) lockedIds = [];

    const count = result[1];
    const activeLockCount = typeof count === 'number' ? count : parseInt(String(count), 10) || 0;

    return { lockedIds, activeLockCount };
}

/** Returns true if lockOwners entry matches the caller's clientKey (and optional runId). */
export function matchesLockOwner(owner: string, clientKey: string, runId?: string | null): boolean {
    const expectedOwner = runId ? `${clientKey}:${runId}` : clientKey;
    return owner === expectedOwner || owner === clientKey;
}

/**
 * Atomic unlock-by-owner: scans lockOwners, unlocks every request whose owner
 * matches the clientKey (and optional runId) using the same rules as the old
 * per-request loop, optionally re-queueing unlocked requests at the forefront.
 * Returns the number of unlocked requests. One round-trip, no race window
 * between ZREM and HDEL, and the whole owners hash never leaves the server.
 */
export const UNLOCK_REQUESTS_LUA = `
local lockedKey = KEYS[1]
local lockOwnersKey = KEYS[2]
local pendingKey = KEYS[3]
local clientKey = ARGV[1]
local runId = ARGV[2]
local forefront = ARGV[3]

local expectedOwner = clientKey
if runId ~= '' then
  expectedOwner = clientKey .. ':' .. runId
end

local clientKeyPrefix = clientKey .. ':'

local flat = redis.call('HGETALL', lockOwnersKey)
local unlockedCount = 0

for i = 1, #flat, 2 do
  local requestId = flat[i]
  local owner = flat[i + 1]
  -- In an Actor run, unlock both (1) requests locked by this
  -- specific run (clientKey:runId) and (2) requests locked by the same
  -- clientKey without run context (clientKey), but NOT requests locked by the
  -- same clientKey in DIFFERENT runs (clientKey:otherRunId).
  if owner == expectedOwner or owner == clientKey
      or (runId == '' and string.sub(owner, 1, #clientKeyPrefix) == clientKeyPrefix) then
    redis.call('ZREM', lockedKey, requestId)
    redis.call('HDEL', lockOwnersKey, requestId)
    if forefront == '1' then
      redis.call('ZADD', pendingKey, 0, requestId)
    end
    unlockedCount = unlockedCount + 1
  end
end

return unlockedCount
`;

export interface UnlockRequestsKeys {
    lockedKey: string;
    lockOwnersKey: string;
    pendingKey: string;
}

export async function unlockRequestsAtomic(
    redis: Redis,
    keys: UnlockRequestsKeys,
    params: { clientKey: string; forefront?: boolean; runId?: string },
): Promise<number> {
    const result = await redis.eval(
        UNLOCK_REQUESTS_LUA,
        3,
        keys.lockedKey,
        keys.lockOwnersKey,
        keys.pendingKey,
        params.clientKey,
        params.runId ?? '',
        params.forefront ? '1' : '0',
    );
    return typeof result === 'number' ? result : parseInt(String(result), 10) || 0;
}
