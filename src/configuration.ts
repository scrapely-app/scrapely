// SPDX-License-Identifier: Apache-2.0
// Derived from apify-sdk-js (src/configuration.ts),
// Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
// Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
import type { ConfigurationOptions as CoreConfigurationOptions } from '@crawlee/core';
import { Configuration as CoreConfiguration } from '@crawlee/core';

export interface ConfigurationOptions extends CoreConfigurationOptions {
    metamorphAfterSleepMillis?: number;
    actorEventsWsUrl?: string;
    token?: string;
    actorId?: string;
    actorRunId?: string;
    actorTaskId?: string;
    apiBaseUrl?: string;
    apiPublicBaseUrl?: string;
    containerPort?: number;
    containerUrl?: string;
    proxyHostname?: string;
    proxyPassword?: string;
    proxyPort?: number;
    proxyStatusUrl?: string;
    /** @deprecated use `containerPort` instead */
    standbyPort?: number;
    standbyUrl?: string;
    isAtHome?: boolean;
    userId?: string;
    inputSecretsPrivateKeyPassphrase?: string;
    inputSecretsPrivateKeyFile?: string;
    maxTotalChargeUsd?: number;
    metaOrigin?: string;
    testPayPerEvent?: boolean;
    useChargingLogDataset?: boolean;
    actorPricingInfo?: string;
    chargedEventCounts?: string;
    actorStoragesJson?: string;
    actorPermissionLevel?: string;
    userIsPaying?: boolean;
}

export class Configuration extends CoreConfiguration {
    static override globalConfig?: Configuration;

    protected static override ENV_MAP = {
        ...CoreConfiguration.ENV_MAP,

        APIFY_AVAILABLE_MEMORY_RATIO: 'availableMemoryRatio',
        APIFY_PURGE_ON_START: 'purgeOnStart',
        APIFY_MEMORY_MBYTES: 'memoryMbytes',
        APIFY_DEFAULT_DATASET_ID: 'defaultDatasetId',
        APIFY_DEFAULT_KEY_VALUE_STORE_ID: 'defaultKeyValueStoreId',
        APIFY_DEFAULT_REQUEST_QUEUE_ID: 'defaultRequestQueueId',
        APIFY_INPUT_KEY: 'inputKey',
        APIFY_PERSIST_STATE_INTERVAL_MILLIS: 'persistStateIntervalMillis',
        APIFY_HEADLESS: 'headless',
        APIFY_XVFB: 'xvfb',
        APIFY_CHROME_EXECUTABLE_PATH: 'chromeExecutablePath',
        APIFY_DEFAULT_BROWSER_PATH: 'defaultBrowserPath',
        APIFY_DISABLE_BROWSER_SANDBOX: 'disableBrowserSandbox',

        APIFY_TOKEN: 'token',
        APIFY_METAMORPH_AFTER_SLEEP_MILLIS: 'metamorphAfterSleepMillis',
        APIFY_TEST_PERSIST_INTERVAL_MILLIS: 'persistStateIntervalMillis',
        APIFY_ACTOR_EVENTS_WS_URL: 'actorEventsWsUrl',
        APIFY_ACTOR_ID: 'actorId',
        APIFY_API_BASE_URL: 'apiBaseUrl',
        APIFY_API_PUBLIC_BASE_URL: 'apiPublicBaseUrl',
        APIFY_IS_AT_HOME: 'isAtHome',
        APIFY_ACTOR_RUN_ID: 'actorRunId',
        APIFY_ACTOR_TASK_ID: 'actorTaskId',
        APIFY_CONTAINER_PORT: 'containerPort',
        APIFY_CONTAINER_URL: 'containerUrl',
        APIFY_USER_ID: 'userId',
        APIFY_USER_IS_PAYING: 'userIsPaying',
        APIFY_PROXY_HOSTNAME: 'proxyHostname',
        APIFY_PROXY_PASSWORD: 'proxyPassword',
        APIFY_PROXY_STATUS_URL: 'proxyStatusUrl',
        APIFY_PROXY_PORT: 'proxyPort',
        APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE: 'inputSecretsPrivateKeyFile',
        APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE: 'inputSecretsPrivateKeyPassphrase',
        APIFY_META_ORIGIN: 'metaOrigin',

        SCRAPELY_TOKEN: 'token',
        SCRAPELY_API_URL: 'apiBaseUrl',
        SCRAPELY_IS_AT_HOME: 'isAtHome',

        ACTOR_DEFAULT_DATASET_ID: 'defaultDatasetId',
        ACTOR_DEFAULT_KEY_VALUE_STORE_ID: 'defaultKeyValueStoreId',
        ACTOR_DEFAULT_REQUEST_QUEUE_ID: 'defaultRequestQueueId',
        ACTOR_EVENTS_WEBSOCKET_URL: 'actorEventsWsUrl',
        ACTOR_ID: 'actorId',
        ACTOR_INPUT_KEY: 'inputKey',
        ACTOR_MEMORY_MBYTES: 'memoryMbytes',
        ACTOR_PERMISSION_LEVEL: 'actorPermissionLevel',
        ACTOR_RUN_ID: 'actorRunId',
        ACTOR_STANDBY_PORT: 'standbyPort',
        ACTOR_STANDBY_URL: 'standbyUrl',
        ACTOR_TASK_ID: 'actorTaskId',
        ACTOR_WEB_SERVER_PORT: 'containerPort',
        ACTOR_WEB_SERVER_URL: 'containerUrl',
        ACTOR_MAX_TOTAL_CHARGE_USD: 'maxTotalChargeUsd',
        ACTOR_TEST_PAY_PER_EVENT: 'testPayPerEvent',
        ACTOR_USE_CHARGING_LOG_DATASET: 'useChargingLogDataset',
        APIFY_ACTOR_PRICING_INFO: 'actorPricingInfo',
        APIFY_CHARGED_ACTOR_EVENT_COUNTS: 'chargedEventCounts',
        ACTOR_STORAGES_JSON: 'actorStoragesJson',
        ACTOR_META_ORIGIN: 'metaOrigin',
    };

    protected static override INTEGER_VARS = [
        ...CoreConfiguration.INTEGER_VARS,
        'proxyPort',
        'containerPort',
        'standbyPort',
        'metamorphAfterSleepMillis',
        'maxTotalChargeUsd',
    ];

    protected static override BOOLEAN_VARS = [
        ...CoreConfiguration.BOOLEAN_VARS,
        'isAtHome',
        'testPayPerEvent',
        'useChargingLogDataset',
        'userIsPaying',
    ];

    protected static override DEFAULTS = {
        ...CoreConfiguration.DEFAULTS,
        defaultKeyValueStoreId: 'default',
        defaultDatasetId: 'default',
        defaultRequestQueueId: 'default',
        inputKey: 'INPUT',
        apiBaseUrl: 'https://api.scrape.ly',
        apiPublicBaseUrl: 'https://api.scrape.ly',
        proxyStatusUrl: 'http://proxy.scrape.ly',
        proxyHostname: 'proxy.scrape.ly',
        proxyPort: 8000,
        containerPort: 4321,
        containerUrl: 'http://localhost:4321',
        standbyPort: 4321,
        metamorphAfterSleepMillis: 300e3,
        persistStateIntervalMillis: 60e3,
        testPayPerEvent: false,
        useChargingLogDataset: false,
    };

    override get<T extends keyof ConfigurationOptions, U extends ConfigurationOptions[T]>(
        key: T,
        defaultValue?: U,
    ): U {
        return super.get(key as keyof CoreConfigurationOptions, defaultValue);
    }

    override set(key: keyof ConfigurationOptions, value?: any) {
        super.set(key as keyof CoreConfigurationOptions, value);
    }

    static override getGlobalConfig(): Configuration {
        if (Configuration.storage.getStore()) {
            return Configuration.storage.getStore() as Configuration;
        }

        Configuration.globalConfig ??= new Configuration();
        return Configuration.globalConfig as Configuration;
    }

    static override resetGlobalState(): void {
        delete this.globalConfig;
    }
}

CoreConfiguration.getGlobalConfig = Configuration.getGlobalConfig;
// @ts-expect-error protected property
CoreConfiguration.ENV_MAP = Configuration.ENV_MAP;
// @ts-expect-error protected property
CoreConfiguration.INTEGER_VARS = Configuration.INTEGER_VARS;
// @ts-expect-error protected property
CoreConfiguration.BOOLEAN_VARS = Configuration.BOOLEAN_VARS;
// @ts-expect-error protected property
CoreConfiguration.DEFAULTS = Configuration.DEFAULTS;
