/**
 * Map Scrapely ACTOR_* log env vars to APIFY_* before @apify/log reads them at module init.
 * Must be imported before any @crawlee/core or @apify/log import.
 */
if (!process.env.APIFY_LOG_LEVEL && process.env.ACTOR_LOG_LEVEL) {
    process.env.APIFY_LOG_LEVEL = process.env.ACTOR_LOG_LEVEL;
}
if (!process.env.APIFY_LOG_FORMAT && process.env.ACTOR_LOG_FORMAT) {
    process.env.APIFY_LOG_FORMAT = process.env.ACTOR_LOG_FORMAT;
}
