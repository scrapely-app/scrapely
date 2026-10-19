/**
 * Package-manager processes in actor containers.
 *
 * The base images preload the SDK's browser patch into every Node process
 * (NODE_OPTIONS=--import /opt/scrapely/register.mjs). `npm start` is two Node
 * processes: npm, which only spawns the start script, and the actor. npm itself
 * costs ~0.6 s at 1 core and ~2.7 s at 0.25 core (1 GB run) on the platform.
 *
 * - npm / npx / yarn / pnpm processes never run actor code, so they get no
 *   loader hooks. Their children inherit NODE_OPTIONS and get the preload.
 * - `npm start` / `npm run <script>` whose script is a plain `node <args>`
 *   (no shell syntax, no pre/post scripts, no .npmrc) runs that node process
 *   directly instead of loading npm: the preload spawns it with the
 *   environment npm would give it, forwards signals and exits with its code.
 *   Set SCRAPELY_NPM_START_SHORTCUT=0 to always use npm.
 *
 * Loaded in every Node process by the preload: keep it free of heavy imports.
 */
import type { ChildProcess } from 'node:child_process';

type PackageManager = 'npm' | 'npx' | 'yarn' | 'pnpm';

/** Main-script basenames of package-manager CLIs (npm's bin, corepack's shims). */
const PACKAGE_MANAGER_SCRIPTS: Record<string, PackageManager> = {
    'npm-cli.js': 'npm',
    'npx-cli.js': 'npx',
    'yarn.js': 'yarn',
    'yarn.cjs': 'yarn',
    'pnpm.cjs': 'pnpm',
    'pnpm.js': 'pnpm',
    'pnpm.mjs': 'pnpm',
};

/** @internal Which package manager this process is running, from its main script. */
export function detectPackageManager(mainScript: string | undefined = process.argv[1]): PackageManager | undefined {
    if (!mainScript) return undefined;
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const path = require('path');
    let resolved = mainScript;
    try {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        resolved = require('fs').realpathSync(mainScript);
    } catch {
        // keep the path as given
    }
    const base = path.basename(resolved);
    const kind = PACKAGE_MANAGER_SCRIPTS[base];
    if (!kind) return undefined;
    // yarn.js / pnpm.cjs are generic names: require a package-manager install path.
    if (kind === 'npm' || kind === 'npx') return /[/\\]npm[/\\]bin[/\\]/.test(resolved) ? kind : undefined;
    return /[/\\](yarn|pnpm|corepack|\.yarn)[/\\]|[/\\]yarn-[^/\\]+[/\\]|[/\\]\.pnpm[/\\]/.test(resolved) ? kind : undefined;
}

/** A script token that needs no shell: no quotes, expansions, globs, operators or redirects. */
const PLAIN_TOKEN_RE = /^[A-Za-z0-9_@%+=:,./-]+$/;

/** npm flags accepted before/after the script name; anything else falls back to npm. */
const QUIET_FLAGS = new Set(['--silent', '-s', '--quiet', '-q', '--loglevel=silent', '--loglevel=error', '--loglevel=warn']);

/** npm config keys that change how (or whether) a script runs: never shortcut when set. */
const SCRIPT_CONFIG_KEYS = [
    'node-options',
    'script-shell',
    'ignore-scripts',
    'scripts-prepend-node-path',
    'workspace',
    'workspaces',
    'include-workspace-root',
    'userconfig',
    'globalconfig',
];
const SCRIPT_CONFIG_ENV_RE = new RegExp(
    `^npm_config_(${[...SCRIPT_CONFIG_KEYS, 'prefix'].map((k) => k.replace(/-/g, '_')).join('|')})$`,
    'i',
);

/**
 * Whether an npmrc file sets any of SCRIPT_CONFIG_KEYS. Other settings
 * (progress=false, loglevel, registry, auth, ...) don't affect running a
 * script — the standard actor Dockerfiles write `progress=false` to ~/.npmrc.
 * Unreadable or unparseable files count as setting them.
 */
function readNpmrc(file: string): { affectsScripts: boolean; prefix?: string } {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const fs = require('fs');
    let text: string;
    try {
        if (!fs.existsSync(file)) return { affectsScripts: false };
        text = fs.readFileSync(file, 'utf8');
    } catch {
        return { affectsScripts: true };
    }
    let prefix: string | undefined;
    for (const raw of text.split(/\r?\n/)) {
        const line = raw.trim();
        if (!line || line.startsWith('#') || line.startsWith(';')) continue;
        if (line.startsWith('[')) return { affectsScripts: true }; // ini sections: unexpected, be safe
        const eq = line.indexOf('=');
        const key = (eq === -1 ? line : line.slice(0, eq)).trim().toLowerCase().replace(/_/g, '-');
        if (SCRIPT_CONFIG_KEYS.includes(key)) return { affectsScripts: true };
        // `prefix` only moves the global npmrc ($prefix/etc/npmrc), e.g. Homebrew's builtin npmrc.
        if (key === 'prefix' && eq !== -1) prefix = line.slice(eq + 1).trim();
    }
    return { affectsScripts: false, prefix };
}

export interface NpmStartPlan {
    /** Node binary and its arguments, as the start script would run them. */
    command: string;
    args: string[];
    env: NodeJS.ProcessEnv;
    cwd: string;
    scriptName: string;
}

/**
 * Plan for running `npm start` / `npm run <script>` without npm, or undefined
 * when npm must run it (anything not provably a plain node invocation).
 *
 * @internal Exported for tests.
 */
export function planNpmStart(
    npmArgs: string[] = process.argv.slice(2),
    options: { cwd?: string; env?: NodeJS.ProcessEnv; execPath?: string; npmCliPath?: string } = {},
): NpmStartPlan | undefined {
    const env = options.env ?? process.env;
    if (env.SCRAPELY_NPM_START_SHORTCUT === '0') return undefined;
    if (Object.keys(env).some((k) => SCRIPT_CONFIG_ENV_RE.test(k))) return undefined;

    // `start [flags] [-- extra]` or `run|run-script <name> [flags] [-- extra]`
    const dashDash = npmArgs.indexOf('--');
    const head = dashDash === -1 ? npmArgs : npmArgs.slice(0, dashDash);
    const extra = dashDash === -1 ? [] : npmArgs.slice(dashDash + 1);
    const positional = head.filter((a) => !a.startsWith('-'));
    if (head.some((a) => a.startsWith('-') && !QUIET_FLAGS.has(a))) return undefined;
    let scriptName: string;
    if (positional.length === 1 && positional[0] === 'start') {
        scriptName = 'start';
    } else if (positional.length === 2 && (positional[0] === 'run' || positional[0] === 'run-script')) {
        scriptName = positional[1];
    } else {
        return undefined;
    }
    if (!extra.every((a) => PLAIN_TOKEN_RE.test(a))) return undefined;

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const fs = require('fs');
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const path = require('path');
    const cwd = options.cwd ?? process.cwd();
    const packageJsonPath = path.join(cwd, 'package.json');
    const execPath = options.execPath ?? process.execPath;
    const npmCliPath = options.npmCliPath ?? process.argv[1];

    // The npmrc files npm reads: builtin (npm's install dir), global
    // ($prefix/etc/npmrc; prefix defaults to node's install dir unless an
    // npmrc moves it), user, project.
    const home = env.HOME || require('os').homedir();
    const builtin = npmCliPath ? readNpmrc(path.join(path.dirname(path.dirname(npmCliPath)), 'npmrc')) : { affectsScripts: false };
    const user = home ? readNpmrc(path.join(home, '.npmrc')) : { affectsScripts: false };
    const project = readNpmrc(path.join(cwd, '.npmrc'));
    const prefix = project.prefix ?? user.prefix ?? builtin.prefix ?? path.dirname(path.dirname(execPath));
    const global = readNpmrc(path.join(prefix, 'etc', 'npmrc'));
    if ([builtin, global, user, project].some((rc) => rc.affectsScripts)) return undefined;

    let pkg: any;
    try {
        pkg = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
    } catch {
        return undefined;
    }
    if (!pkg || typeof pkg !== 'object' || pkg.workspaces) return undefined;
    const scripts = pkg.scripts ?? {};
    const script = scripts[scriptName];
    if (typeof script !== 'string') return undefined;
    if (scripts[`pre${scriptName}`] !== undefined || scripts[`post${scriptName}`] !== undefined) return undefined;

    const tokens = script.trim().split(/[ \t]+/);
    if (tokens[0] !== 'node' || tokens.length < 2) return undefined;
    if (!tokens.every((t) => PLAIN_TOKEN_RE.test(t))) return undefined;

    const silent = head.some((a) => QUIET_FLAGS.has(a));

    // The script environment npm provides (@npmcli/run-script): local bins on
    // PATH, package metadata, lifecycle names.
    const binDirs: string[] = [];
    for (let dir = cwd; ; ) {
        binDirs.push(path.join(dir, 'node_modules', '.bin'));
        const parent = path.dirname(dir);
        if (parent === dir) break;
        dir = parent;
    }
    // A local `node` binary would win on npm's PATH.
    if (binDirs.some((dir) => fs.existsSync(path.join(dir, 'node')))) return undefined;
    const childEnv: NodeJS.ProcessEnv = {
        ...env,
        PATH: [...binDirs, env.PATH].filter(Boolean).join(path.delimiter),
        INIT_CWD: env.INIT_CWD ?? cwd,
        NODE: execPath,
        npm_node_execpath: execPath,
        npm_lifecycle_event: scriptName,
        npm_lifecycle_script: script,
        npm_package_json: packageJsonPath,
        npm_command: scriptName === 'start' ? 'start' : 'run-script',
    };
    if (npmCliPath) childEnv.npm_execpath = npmCliPath;
    if (typeof pkg.name === 'string') childEnv.npm_package_name = pkg.name;
    if (typeof pkg.version === 'string') childEnv.npm_package_version = pkg.version;
    if (silent) childEnv.npm_config_loglevel = 'silent';

    return { command: execPath, args: [...tokens.slice(1), ...extra], env: childEnv, cwd, scriptName };
}

const FORWARDED_SIGNALS: NodeJS.Signals[] = ['SIGTERM', 'SIGINT', 'SIGHUP', 'SIGQUIT', 'SIGUSR2'];

/**
 * Run the planned node process in place of npm: inherit stdio, forward
 * signals, exit with its exit code (128 + signal number when it was killed).
 *
 * @internal Exported for tests.
 */
export function runNpmStartPlan(plan: NpmStartPlan): ChildProcess {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { spawn } = require('child_process');
    const child: ChildProcess = spawn(plan.command, plan.args, { cwd: plan.cwd, env: plan.env, stdio: 'inherit' });
    const forward = (signal: NodeJS.Signals) => {
        try {
            child.kill(signal);
        } catch {
            // already exited
        }
    };
    for (const signal of FORWARDED_SIGNALS) process.on(signal, forward);
    child.on('error', (err: Error) => {
        console.error(`[scrapely] could not start '${plan.command} ${plan.args.join(' ')}': ${err.message}`);
        process.exit(1);
    });
    child.on('exit', (code: number | null, signal: NodeJS.Signals | null) => {
        for (const s of FORWARDED_SIGNALS) process.removeListener(s, forward);
        if (code !== null) process.exit(code);
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const signals = require('os').constants.signals as Record<string, number>;
        process.exit(128 + (signal ? signals[signal] ?? 1 : 1));
    });
    return child;
}

/**
 * Called by the preload in every Node process. Returns true for
 * package-manager processes (no loader hooks needed). For a shortcut-able
 * `npm start`, the main module load (npm-cli.js) is intercepted: the start
 * script's node process runs instead and npm itself is never loaded. If Node
 * ever loaded the main module another way, npm simply runs as before.
 */
export function handlePackageManagerProcess(): boolean {
    const manager = detectPackageManager();
    if (!manager) return false;
    if (manager !== 'npm') return true;

    let plan: NpmStartPlan | undefined;
    try {
        plan = planNpmStart();
    } catch {
        plan = undefined;
    }
    if (!plan) return true;

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const Module: any = require('module');
    const origLoad = Module._load;
    if (typeof origLoad !== 'function') return true;
    const planned = plan;
    Module._load = function (this: unknown, request: unknown, parent: unknown, isMain: boolean) {
        if (isMain) {
            Module._load = origLoad;
            runNpmStartPlan(planned);
            return {};
        }
        return origLoad.apply(this, [request, parent, isMain]);
    };
    return true;
}
