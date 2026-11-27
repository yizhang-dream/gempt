import assert from 'node:assert/strict';
import childProcess from 'node:child_process';
import { execFile, spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { serveAppServerSocket, threadFixture } from './app-server-fixture.mjs';
import { once } from 'node:events';
import { appendFileSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync,
  realpathSync, rmSync, statSync, symlinkSync, utimesSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { syncBuiltinESMExports } from 'node:module';
import { homedir, tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { createInterface } from 'node:readline';
import { test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { runInNewContext } from 'node:vm';
import { installAgy, resolveNodeExecutable, stagePluginBundle, syncInPlace, validatePluginBundle } from '../core/agy-install.mjs';
import { buildTierView, GEMPT_VERSION, setEffortSettings } from '../core/config.mjs';
import { assertSafeOutDir } from '../scripts/build-agy-plugin.mjs';
import { contextPath, readHostContext, updateHostContext } from '../core/host-context.mjs';
import { handleSwarmAction, handleBoardAction } from '../core/swarm-engine.mjs';
import { ensureSwarm, getRunLineage, getWorker, postBoardMessage, readBoardMessages, upsertWorker } from '../core/swarm-bus.mjs';
import { runLiveOperation, workspaceRun } from '../core/live-service.mjs';
import { handleTool, TOOLS } from '../core/mcp-server.mjs';
import { buildWaitCommand, dispatchRun, interruptRun, isRecentUnconfirmedRun, listActiveRoster, readRun,
  reconcileRunState, replyToolCall, runDirectory, runWithinRoot, saveRun, steerRun, waitForRun, waitForRuns } from '../core/runs.mjs';
import { waitForBoard } from '../core/wait.mjs';
import { handleBoardMcpTool } from '../core/board-mcp.mjs';
import { DEFAULT_LOCK_STALE_MS, isValidSessionId, lockStaleMsForBudget, parseToolArguments, validateSessionId, withLock } from '../core/state.mjs';
import { linkTarget, listArguments, streamRenderKey, timelineMessages } from '../mcp_app/live-state.js';
import { assertShareWorktree, ensureShareWorktree, pruneShareWorktrees,
  removeShareWorktree, resolveExternalWorktreeSourceCwd, workspaceFileRoots, worktreesRoot } from '../core/worktrees.mjs';
import { createAppServerManager } from '../magpie_plugin/lib/app-server-manager.mjs';
import { probeGatewaySettings, readOverview, readRoutingSummary, readState, resetSettings, updateSettings } from '../magpie_plugin/lib/backend.mjs';
import { dispatchPageApi } from '../magpie_plugin/lib/page-api.mjs';
import { deadlineForTimeout, scheduleTimeout } from '../core/timeout.mjs';
import { resolveExecTimeoutSec, validateExecTimeoutSec } from '../magpie_plugin/lib/routing.mjs';
import { runAppServer } from '../magpie_plugin/lib/app-server-run.mjs';

const execFileAsync = promisify(execFile);
const hookPath = fileURLToPath(new URL('../plugin/work_split_hook.mjs', import.meta.url));
const cliPath = fileURLToPath(new URL('../bin/gempt.mjs', import.meta.url));
const launcherUrl = new URL('../core/assets/mcp-app/launcher.html', import.meta.url);
const launcherResource = { uri: 'ui://gempt/launcher.html', mimeType: 'text/html;profile=mcp-app',
  _meta: { ui: { prefersBorder: false } } };

test('inline launcher negotiates the Hub bridge, opens once for fresh results and shows recovery on errors', () => {
  const source = readFileSync(launcherUrl, 'utf8').match(/<script>([\s\S]+)<\/script>/)[1];
  const toggle = 'com.google.antigravity/toggle-aux-pane';
  const now = 90_000;
  const messages = [];
  const callbacks = {};
  const elements = Object.fromEntries(['card', 'title', 'badge', 'hint', 'open'].map((id) => [id, {
    offsetHeight: 48,
    toggleAttribute() {},
    addEventListener: (name, handler) => { callbacks[name] = handler; },
  }]));
  const variables = new Map();
  const root = { dataset: {}, style: { setProperty: variables.set.bind(variables), removeProperty: variables.delete.bind(variables) } };
  const parent = { postMessage(message, origin) { assert.equal(origin, '*'); messages.push(JSON.parse(JSON.stringify(message))); } };
  runInNewContext(source, {
    window: { parent, addEventListener: (name, handler) => { callbacks[name] = handler; } },
    document: { documentElement: root, getElementById: (id) => elements[id] },
    Date: { now: () => now }, setTimeout() {}, clearTimeout() {},
  });
  const emit = (data, sender = parent) => callbacks.message({ source: sender, data: { jsonrpc: '2.0', ...data } });
  const opens = () => messages.filter(({ method }) => method === toggle);
  assert.deepEqual(messages[0], {
    jsonrpc: '2.0', id: 1, method: 'ui/initialize',
    params: { protocolVersion: '2026-01-26', appInfo: { name: 'GemPT launcher', version: '1.0.0' }, appCapabilities: {} },
  });
  emit({ id: 1, result: { protocolVersion: '2026-01-26', hostInfo: { name: 'Antigravity', version: 'test' },
    hostCapabilities: {}, hostContext: { theme: 'dark', locale: 'zh-CN', styles: { variables: { '--color-text-primary': '#eee' } } } } });
  assert.deepEqual(messages.slice(1), [
    { jsonrpc: '2.0', method: 'ui/notifications/initialized' },
    { jsonrpc: '2.0', method: 'ui/notifications/size-changed', params: { height: 48 } },
  ]);
  assert.equal(root.dataset.theme, 'dark');
  assert.equal(elements.open.textContent, '打开 GemPT');
  assert.equal(variables.get('--color-text-primary'), '#eee');
  emit({ method: 'ui/notifications/tool-input', params: { arguments: { title: 'Starting work' } } });
  assert.equal(elements.title.textContent, 'Starting work');
  const result = (structuredContent, sender) => emit({
    method: 'ui/notifications/tool-result', params: { content: [], structuredContent },
  }, sender);
  const fresh = { kind: 'swarm', title: 'Actual work', member_count: 2, auto_open_panel: true, created_at_ms: now - 1 };
  result({ ...fresh, created_at_ms: now - 30_000 });
  result({ ...fresh, created_at_ms: now + 1 });
  result({ ...fresh, auto_open_panel: false });
  result(fresh, {});
  assert.equal(opens().length, 0);
  result(fresh);
  result(fresh);
  assert.equal(opens().length, 1);
  assert.deepEqual(opens()[0].params, { tabType: 'sidecarView', uri: 'sidecar://gempt/gempt_live/' });
  assert.equal(elements.title.textContent, 'Actual work');
  assert.equal(elements.badge.textContent, '2 名成员');
  emit({ id: opens()[0].id, result: { isError: true } });
  assert.match(elements.hint.textContent, /侧边栏/);
  emit({ method: 'ui/notifications/host-context-changed', params: { theme: 'light', locale: 'en-US', styles: { variables: {} } } });
  assert.equal(root.dataset.theme, 'light');
  assert.equal(variables.size, 0);
  assert.match(elements.hint.textContent, /side panel/);
  callbacks.click();
  assert.equal(opens().length, 2);
  assert.deepEqual(opens()[1].params, opens()[0].params);
  emit({ id: opens()[1].id, error: { code: -32601, message: 'Method not found' } });
  assert.equal(elements.hint.hidden, false);
  callbacks.click();
  emit({ id: opens()[2].id, result: {} });
  assert.equal(elements.hint.hidden, true);
  emit({ id: 'hub-ping', method: 'ping' });
  assert.deepEqual(messages.at(-1), { jsonrpc: '2.0', id: 'hub-ping', result: {} });
});

test('Live builds the split pane and preserves host scope, safe link routes and exact transcript text', () => {
  const html = readFileSync(new URL('../core/assets/http-ui/index.html', import.meta.url), 'utf8');
  const css = readFileSync(new URL('../core/assets/http-ui/styles.css', import.meta.url), 'utf8');
  const js = readFileSync(new URL('../core/assets/http-ui/app.js', import.meta.url), 'utf8');
  const styles = readFileSync(new URL('../mcp_app/styles.css', import.meta.url), 'utf8').replace(/\r\n/g, '\n');
  assert.doesNotMatch(html, /<meta\s+http-equiv=["']Content-Security-Policy["']/i);
  assert.match(html, /<link rel="stylesheet" href="\.\/styles\.css">/);
  assert.match(html, /<script defer src="\.\/app\.js"><\/script>/);
  assert.ok(css.includes(styles));
  assert.match(css, /@media \(min-width: 900px\)[\s\S]+?grid-template-columns: 290px minmax\(0, 1fr\)/);
  assert.match(css, /max-width: 760px; margin: 0 auto/);
  assert.match(html, /id="scope-all"[^>]+>全部<\/button>/);
  assert.match(js, /createTHead\(/);

  assert.deepEqual(listArguments('session', null), { scope: 'session', limit: 50 });
  assert.deepEqual(listArguments('session', 'selected'), { scope: 'session', limit: 50, host_conversation_id: 'selected' });
  assert.deepEqual(listArguments('all', 'selected', 50), { scope: 'all', limit: 50, offset: 50 });
  assert.deepEqual(linkTarget('https://example.org/test?q=a&b=c'), { kind: 'web', value: 'https://example.org/test?q=a&b=c' });
  assert.equal(linkTarget('file:///C:/workspace/notes.md'), null);
  assert.deepEqual(linkTarget('file:///C:/workspace/notes.md', null, { approved: true }), { kind: 'file', value: 'file:///C:/workspace/notes.md' });
  assert.deepEqual(linkTarget('file:///C:/workspace/notes.md', null, { fileUris: { 'notes.md': 'file:///C:/workspace/notes.md' } }),
    { kind: 'file', value: 'file:///C:/workspace/notes.md' });
  assert.deepEqual(linkTarget('src/file name.md#L8', 'D:\\gempt'), { kind: 'file', value: 'file:///D:/gempt/src/file%20name.md#L8' });
  assert.equal(linkTarget('C:\\workspace\\notes.md', 'D:\\gempt'), null);
  assert.equal(linkTarget('../outside.md', 'D:\\gempt'), null);
  for (const target of ['javascript:alert(1)', 'data:text/html,x', '//example.org/file', 'file://example.org/share/file']) {
    assert.equal(linkTarget(target, 'D:/gempt'), null);
  }

  const first = { id: 'one', type: 'agent_message', text: 'First' };
  const second = { id: 'two', type: 'agent_message', text: 'Second' };
  const events = [first, { type: 'command', output: 'old' }, second];
  assert.deepEqual([...timelineMessages(events, 'First paragraph\n\nSecond paragraph')], [
    [first, 'First paragraph'], [second, 'Second paragraph'],
  ]);
  assert.deepEqual([...timelineMessages(events, 'Prior hidden message\n\nFirst\n\nSecond')], [
    [second, 'Prior hidden message\n\nFirst\n\nSecond'],
  ]);
  const key = (timeline, text) => streamRenderKey('id', 'running', timeline, {}, { text, complete: true }, null, true);
  assert.notEqual(key(events, 'same'), key(events, 'size'));
  assert.notEqual(key(events, 'same'), key([first, { type: 'command', output: 'new' }, second], 'same'));
});

function fixture(t) {
  const root = realpathSync.native(mkdtempSync(join(tmpdir(), 'gempt-node-')));
  // Detached workers can still be closing the SQLite WAL or their diagnostic log.
  t.after(() => rmSync(root, { recursive: true, force: true, maxRetries: 8, retryDelay: 25 }));
  const configPath = join(root, 'config', 'config.json');
  const sessionsDir = join(root, 'sessions');
  const cwd = join(root, 'repo');
  mkdirSync(cwd);
  return { root, cwd, configPath, sessionsDir, env: { ...process.env, GEMPT_CONFIG: configPath,
    GEMPT_DISABLE_CIM: '1', GEMPT_SESSIONS_DIR: sessionsDir, GEMPT_WORKTREES_DIR: join(root, 'worktrees'), GEMPT_WORKSPACE_ROOT: cwd } };
}

function jsonProcess(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: 'utf8', timeout: 15_000, windowsHide: true, ...options });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  return JSON.parse(result.stdout);
}

test('Node hook emits entry banners and one user-only alert per context milestone', (t) => {
  const f = fixture(t);
  mkdirSync(dirname(f.configPath));
  writeFileSync(join(dirname(f.configPath), 'app-server.json'),
    JSON.stringify({ status: 'running', pid: process.pid, ws_url: 'ws://127.0.0.1:1' }));
  const env = { ...f.env, GEMPT_APP_SERVER_STATE: join(dirname(f.configPath), 'app-server.json') };
  const invoke = (payload) => jsonProcess(process.execPath, [hookPath], { env, input: JSON.stringify(payload) });
  for (const [profile, share] of [['gemini', 0], ['gemini_first', 15], ['balanced', 35], ['gpt', 60], ['gpt_first', 80]]) {
    writeFileSync(f.configPath, JSON.stringify({ routing_policy: { work_split: profile } }));
    const payload = { conversationId: profile, workspacePaths: [f.cwd], modelName: 'gemini-4-neon-high' };
    const start = invoke({ ...payload, hookEventName: 'SessionStart' });
    assert.deepEqual(invoke({ ...payload, hookEventName: 'SessionStart' }), {});
    if (share) assert.ok(start.injectSteps[0].systemMessage.systemMessage.length < 256);
    if (share) assert.match(start.injectSteps[0].systemMessage.systemMessage, new RegExp(`${share}% GPT, family=neon`));
    else assert.deepEqual(start, {});
    const turn = { ...payload, hookEventName: 'PreInvocation', initialNumSteps: 31, lastUserInput: 'Review this' };
    const entry = invoke({ ...turn, invocationNum: 0 });
    assert.deepEqual(entry, {});
    assert.deepEqual(invoke({ ...turn, invocationNum: 0, lastUserInput: 'A different turn' }), {});
    if (share === 35) {
      writeFileSync(f.configPath, JSON.stringify({ routing_policy: { work_split: 'gpt' } }));
      assert.match(invoke({ ...turn, invocationNum: 2 }).injectSteps[0].systemMessage.systemMessage, /60%/);
    }
    assert.deepEqual(invoke({ ...turn, invocationNum: 1 }), {});
  }
  assert.equal(readHostContext(f.configPath).conversation_id, 'gpt_first');
  const transcriptPath = join(f.root, 'transcript.jsonl');
  const turn = { hookEventName: 'PreInvocation', conversationId: 'long-chat', invocationNum: 7, transcriptPath };
  for (const tokens of [120, 200, 280]) {
    writeFileSync(transcriptPath, `${'x'.repeat(70_000)}\n${JSON.stringify({ type: 'PLANNER_RESPONSE', cache_read_tokens: tokens * 1000 })}\n`);
    const { systemMessage } = invoke(turn).injectSteps[0];
    assert.equal(systemMessage.systemMessage, '');
    assert.match(systemMessage.metadata.user_markdown, new RegExp(`${tokens}k.*\\/compact`));
    assert.deepEqual(invoke(turn), {});
  }
});

test('hook counts each explicit bypass turn once across invocations and tool calls', async (t) => {
  const f = fixture(t);
  mkdirSync(dirname(f.configPath));
  writeFileSync(f.configPath, JSON.stringify({ routing_policy: { work_split: 'gpt_first' } }));
  const invoke = fields => jsonProcess(process.execPath, [hookPath], { env: f.env,
    input: JSON.stringify({ conversationId: 'counted', lastUserInput: '自己查', ...fields }) });
  const tool = { hookEventName: 'PreToolUse', toolCall: { name: 'read_file', args: { Path: 'app.mjs' } } };
  for (const initialNumSteps of [3, 7]) {
    for (const invocationNum of [0, 1]) invoke({ hookEventName: 'PreInvocation', initialNumSteps, invocationNum });
    invoke(tool);
    invoke(tool);
  }
  invoke({ ...tool, lastUserInput: 'review' });
  const saved = JSON.parse(readFileSync(join(dirname(f.configPath), 'hook-state', 'counted.json')));
  assert.equal(saved.bypass_count, 2);
  assert.equal((await readOverview(f)).activity_7d.bypass_count, 2);
});

test('Node hook applies write and explore gates with exemptions, resets and fail-open', (t) => {
  const f = fixture(t);
  mkdirSync(dirname(f.configPath));
  const health = join(f.root, 'app-server.json');
  const env = { ...f.env, GEMPT_APP_SERVER_STATE: health, GEMPT_CIRCUIT_WINDOW_SEC: '600' };
  const set = (work_split) => writeFileSync(f.configPath, JSON.stringify({ routing_policy: { work_split } }));
  const allow = (name, args, payload) => jsonProcess(process.execPath, [hookPath], { env,
    input: JSON.stringify({ hookEventName: 'PreToolUse', conversationId: 'turn', workspacePaths: [f.cwd],
      lastUserInput: 'Change this', toolCall: { name, args }, ...payload }) });
  const edit = (TargetFile = join(f.cwd, 'app.mjs'), Content = 'code', payload) => allow('edit_file', { TargetFile, Content }, payload);
  set('balanced');
  assert.equal(edit().decision, 'allow');
  writeFileSync(health, JSON.stringify({ status: 'running', pid: process.pid, ws_url: 'ws://127.0.0.1:1' }));
  assert.match(edit().reason, /35% Write Gate/);
  assert.equal(edit(join(f.cwd, 'app.json'), 'line\n'.repeat(15)).decision, 'allow');
  assert.match(edit(join(f.cwd, 'app.json'), 'line\n'.repeat(16)).reason, /Write Gate/);
  assert.equal(edit(join(f.cwd, 'README.md')).decision, 'allow');
  assert.equal(edit(join(f.cwd, '.gemini', 'antigravity', 'brain', 'prototype.js')).decision, 'allow');
  assert.equal(edit(undefined, undefined, { lastUserInput: '这里你自己改' }).decision, 'allow');
  for (const profile of ['gemini', 'gemini_first']) {
    set(profile);
    assert.equal(edit().decision, 'allow');
  }
  for (const [profile, share, limit] of [['gpt', 60, 5], ['gpt_first', 80, 2]]) {
    set(profile);
    const turn = { lastUserInput: profile };
    const probes = [
      ['list_dir', {}], ['find_by_name', {}], ['run_command', { CommandLine: 'git status' }],
      ['run_command', { CommandLine: 'rg --files' }],
      ['run_command', { CommandLine: 'git branch && git log --oneline || git rev-parse HEAD; git tag\nls | dir; Get-ChildItem; gci; tree' }],
    ];
    for (const probe of probes) assert.equal(allow(...probe, turn).decision, 'allow');
    const external = join(f.root, 'repo-sibling', 'app.mjs');
    for (const probe of [
      ['read_file', { Path: external }],
      ['grep_search', { SearchPath: dirname(external) }],
      ['view_file_outline', { AbsolutePath: external }],
      ['run_command', { CommandLine: `git status && cat "${external}"` }],
      ['run_command', { CommandLine: `rg -n pattern "${dirname(external)}"` }],
      ['run_command', { CommandLine: `head -n 40 "${external}" && sed -n '1,10p' "${external}"` }],
    ]) assert.equal(allow(...probe, turn).decision, 'allow');
    const read = () => allow('read_file', { Path: join(f.cwd, 'app.mjs') }, { lastUserInput: profile });
    for (let count = 0; count < limit; count++) assert.equal(read().decision, 'allow');
    for (const probe of probes) assert.equal(allow(...probe, turn).decision, 'allow');
    for (const [index, probe] of [
      ['grep_search', {}], ['run_command', { CommandLine: 'git diff' }],
      ['run_command', { CommandLine: 'git status && cat foo.mjs' }],
      ['run_command', { CommandLine: 'rg --files | Select-String app' }],
      ['run_command', { CommandLine: 'git log -p' }],
      ['run_command', { CommandLine: 'git status -v' }],
      ['run_command', { CommandLine: 'git status || rg pattern; gempt status --wait' }],
    ].entries()) assert.match(allow(...probe, turn).reason, new RegExp((limit + index + 1) + ' workspace content'));
    const reason = read().reason;
    assert.ok(reason.startsWith(`[GemPT ${share}% Explore Stop-Loss] `));
    assert.match(reason, new RegExp(`limit ${limit}`));
    if (profile === 'gpt_first') assert.match(reason, /1–5 known files.*synchronous gpt_ask\(file_paths\).*single-track gpt_swarm or gpt_run/);
    assert.equal(allow('read_file', { Path: external }, turn).decision, 'allow');
    assert.equal(allow('run_command', { CommandLine: `cat "${external}" app.mjs` }, turn).decision, 'deny');
    assert.equal(allow('run_command', { CommandLine: 'gempt status --run-id 123 --wait' }).decision, 'allow');
    for (const delegation of [
      ['call_mcp_tool', { ServerName: 'gempt_gempt', ToolName: 'gpt_run' }],
      ['call_mcp_tool', { ServerName: 'gempt_gempt', ToolName: 'gpt_swarm' }],
    ]) {
      assert.equal(allow(...delegation, { lastUserInput: profile }).decision, 'allow');
      for (let count = 0; count < limit; count++) assert.equal(read().decision, 'allow');
      assert.equal(read().decision, 'deny');
    }
  }
  state(f, 'a'.repeat(32), { state: 'failed' });
  assert.equal(edit().decision, 'deny');
  state(f, 'b'.repeat(32), { state: 'outcome_unknown' });
  assert.equal(edit().decision, 'allow');
});

test('Antigravity installer replaces a legacy target and stages a runnable Node-only plugin and wait shims', async (t) => {
  const f = fixture(t);
  const target = join(f.root, 'plugins with spaces', 'gempt');
  const binDir = join(f.root, 'bin with spaces');
  const legacyLock = join(dirname(target), '.gempt-install.lock');
  const legacyWait = join(binDir, 'gempt-wait.py');
  mkdirSync(join(target, 'agents'), { recursive: true });
  writeFileSync(join(target, 'agents', 'gempt-worker.md'), 'legacy worker');
  mkdirSync(join(target, 'runtime', 'gempt'), { recursive: true });
  mkdirSync(binDir);
  writeFileSync(legacyLock, '');
  writeFileSync(legacyWait, 'legacy');
  writeFileSync(join(target, 'plugin.json'), JSON.stringify({ name: 'gempt' }));
  writeFileSync(join(target, 'work_split_hook.py'), 'legacy');
  writeFileSync(join(target, 'runtime', 'gempt', 'mcp_server.py'), 'legacy');
  const refreshCalls = [];
  const languageServer = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    refreshCalls.push({ url: req.url, method: req.method, token: req.headers['x-codeium-csrf-token'],
      data: JSON.parse(Buffer.concat(chunks)) });
    res.end('{}');
  });
  languageServer.listen(0, '127.0.0.1');
  await once(languageServer, 'listening');
  t.after(() => languageServer.close());
  const env = { ...f.env, ANTIGRAVITY_CSRF_TOKEN: 'fixture', ANTIGRAVITY_LS_ADDRESS: `127.0.0.1:${languageServer.address().port}` };
  assert.equal(resolveNodeExecutable({ execPath: join(f.root, 'magpie-bun'), env: { PATH: dirname(process.execPath) } }), process.execPath);
  const macHome = join(f.root, 'mac-home');
  const nvmOld = join(macHome, '.nvm', 'versions', 'node', 'v18.19.0', 'bin');
  const nvmNew = join(macHome, '.nvm', 'versions', 'node', 'v22.2.0', 'bin');
  mkdirSync(nvmOld, { recursive: true });
  mkdirSync(nvmNew, { recursive: true });
  writeFileSync(join(nvmOld, 'node'), '#!/bin/sh\n', { mode: 0o755 });
  writeFileSync(join(nvmNew, 'node'), '#!/bin/sh\n', { mode: 0o755 });
  const macOptions = { platform: 'darwin', home: macHome, systemDirs: [] };
  assert.equal(resolveNodeExecutable({ execPath: join(f.root, 'magpie-bun'), env: { PATH: '' }, ...macOptions }), join(nvmNew, 'node'));
  const { findCodexCli } = await import('../magpie_plugin/lib/app-server-manager.mjs');
  writeFileSync(join(nvmNew, 'codex'), '#!/bin/sh\n', { mode: 0o755 });
  assert.equal(findCodexCli('', { PATH: '' }, macOptions), join(nvmNew, 'codex'));
  const installed = await installAgy({ target, binDir, env });
  assert.equal(installed.installed, true);
  assert.ok(!existsSync(legacyLock));
  assert.ok(!existsSync(legacyWait));
  assert.ok(!existsSync(join(target, 'agents', 'gempt-worker.md')));
  assert.ok(existsSync(join(binDir, 'gempt-wait.mjs')));
  const cliShim = join(binDir, process.platform === 'win32' ? 'gempt.cmd' : 'gempt');
  assert.ok(installed.cli_shims.includes(cliShim));
  const help = process.platform === 'win32'
    ? spawnSync(process.env.ComSpec || 'cmd.exe', ['/d', '/c', 'gempt.cmd --help'], { cwd: binDir, env, encoding: 'utf8' })
    : spawnSync(cliShim, ['--help'], { cwd: f.root, env, encoding: 'utf8' });
  assert.equal(help.status, 0, help.stderr || help.error?.message);
  assert.match(help.stdout, /\|run\|ask\|/);
  assert.ok(installed.refresh.refreshed);
  assert.deepEqual(refreshCalls, [{ url: '/exa.language_server_pb.LanguageServerService/RefreshMcpServers',
    method: 'POST', token: 'fixture', data: { serverName: 'gempt_gempt' } }]);
  const bridge = join(target, 'work_split_hook.py');
  assert.equal(readFileSync(bridge, 'utf8').trimEnd().split('\n').length, 7);
  assert.ok(!existsSync(join(target, 'runtime', 'gempt')));
  assert.deepEqual(JSON.parse(readFileSync(join(target, 'runtime', 'package.json'), 'utf8')), { type: 'module', version: '0.10.1' });
  const bundledSkills = new URL('../plugin/skills/', import.meta.url);
  assert.deepEqual(readdirSync(bundledSkills).sort(), ['gempt', 'gempt-chore']);
  assert.deepEqual(readdirSync(join(target, 'skills')).sort(), ['gempt', 'gempt-chore']);
  const config = JSON.parse(readFileSync(join(target, 'mcp_config.json'), 'utf8'));
  const { command, args } = config.mcpServers.gempt;
  assert.equal(command, process.execPath);
  const sidecar = JSON.parse(readFileSync(join(target, 'sidecars', 'gempt_live', 'sidecar.json'), 'utf8'));
  const manifest = JSON.parse(readFileSync(join(target, 'plugin.json'), 'utf8'));
  assert.equal(manifest.displayName, 'GemPT');
  assert.equal(sidecar.display_name, 'GemPT');
  assert.deepEqual(sidecar.ui_config, { display_name: 'GemPT', views: [
    { path: '/', entrypoint: 'SIDECAR_UI_ENTRYPOINT_AUX_PANE', title: 'GemPT' },
  ] });
  assert.equal(sidecar.command, command);
  assert.deepEqual(sidecar.args, ['main.mjs']);
  assert.deepEqual(args, [join(target, 'runtime', 'core', 'mcp-server.mjs')]);
  const initialized = jsonProcess(command, args, { cwd: f.root, env, input: JSON.stringify({
    jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05' },
  }) });
  assert.equal(initialized.result.serverInfo.version, '0.10.1');
  assert.deepEqual(initialized.result.capabilities.resources, {});
  const listedResources = jsonProcess(command, args, { cwd: f.root, env, input: JSON.stringify({
    jsonrpc: '2.0', id: 1, method: 'resources/list',
  }) });
  assert.deepEqual(listedResources.result.resources, [{ ...launcherResource, name: 'GemPT launcher' }]);
  const ctlStatus = jsonProcess(command, args, { cwd: f.root, env, input: JSON.stringify({
    jsonrpc: '2.0', id: 1, method: 'tools/call',
    params: { name: 'gpt_ctl', arguments: { action: 'status' } },
  }) });
  assert.equal(ctlStatus.error, undefined);
  assert.notEqual(ctlStatus.result?.isError, true, ctlStatus.result?.content?.[0]?.text);
  assert.equal(typeof JSON.parse(ctlStatus.result.content[0].text), 'object');
  const rulesText = readFileSync(join(target, 'rules', 'AGENTS.md'), 'utf8');
  assert.ok(rulesText.trim());
  const hooks = JSON.parse(readFileSync(join(target, 'hooks.json'), 'utf8'));
  const matcher = hooks['gempt-work-split'].PreToolUse[0].matcher.split('|');
  assert.ok(matcher.includes('call_mcp_tool'));
  assert.ok(matcher.includes('run_command'));
  assert.ok(!matcher.includes('invoke_subagent'));
  const hook = hooks['gempt-work-split'].SessionStart[0].command;
  assert.ok(hook.includes('work_split_hook.mjs'));
  assert.doesNotMatch(hook, /python/i);
  const hookResult = process.platform === 'win32'
    ? jsonProcess(command, [join(target, 'work_split_hook.mjs')], { cwd: f.root, env, input: '{"hookEventName":"SessionStart"}' })
    : jsonProcess('sh', ['-c', hook], { cwd: f.root, env, input: '{"hookEventName":"SessionStart"}' });
  assert.ok(hookResult.injectSteps[0].systemMessage.systemMessage.startsWith(`GemPT v${GEMPT_VERSION} active:`));
  await t.test('cached Python hook forwards SessionStart to Node', (t) => {
    const python = [process.env.PYTHON_BIN, 'python3', 'python'].filter(Boolean)
      .find((candidate) => spawnSync(candidate, ['-I', '-c', 'import sys'], { windowsHide: true }).status === 0);
    if (!python) return t.skip('A legacy Python launcher is needed to exercise the compatibility bridge.');
    const throughBridge = jsonProcess(python, ['-I', bridge], { cwd: f.root, env, input: '{"hookEventName":"SessionStart"}' });
    assert.deepEqual(throughBridge, hookResult);
  });
  const first = 'a'.repeat(32);
  const second = 'b'.repeat(32);
  state(f, first, { state: 'finished' });
  state(f, second, { state: 'finished' });
  const shim = join(binDir, 'gempt-wait');
  const runShim = (args) => process.platform === 'win32'
    ? jsonProcess(command, [`${shim}.mjs`, ...args], { cwd: f.root, env })
    : jsonProcess(shim, args, { cwd: f.root, env });
  assert.equal(runShim([first]).run_id, first);
  assert.equal(runShim(['status', '--run-id', first, '--wait']).state, 'finished');
  assert.deepEqual(runShim([`${first},${second}`]).ready, [first, second]);
  assert.deepEqual(runShim(['--all', `${first},${second}`]).ready, [first, second]);
  const source = join(f.root, 'invalid plugin');
  mkdirSync(source);
  for (const name of ['hooks.json', 'mcp_config.json', 'work_split_hook.mjs']) {
    copyFileSync(join(dirname(hookPath), name), join(source, name));
  }
  writeFileSync(join(source, 'plugin.json'), JSON.stringify({ name: 'wrong-plugin' }));
  await assert.rejects(installAgy({ source, target, binDir, env }), /manifest name 'gempt'/);
  assert.deepEqual(JSON.parse(readFileSync(join(target, 'mcp_config.json'), 'utf8')), config);
});

test('Antigravity installer resets Windows plugin and shim ACLs after placement, even if icacls is unavailable', async (t) => {
  const f = fixture(t);
  const target = join(f.cwd, 'plugin with spaces');
  const binDir = join(f.cwd, 'bin with spaces');
  const platform = Object.getOwnPropertyDescriptor(process, 'platform');
  let installWindowsAgy;
  try {
    Object.defineProperty(process, 'platform', { value: 'win32' });
    ({ installAgy: installWindowsAgy } = await import('../core/agy-install.mjs?test-windows-acls'));
  } finally {
    Object.defineProperty(process, 'platform', platform);
  }

  const calls = [];
  const mock = t.mock.method(childProcess, 'spawnSync', (command, args, options) => {
    calls.push({
      command: basename(command), args, options,
      exists: existsSync(args[0]), pluginPlaced: existsSync(join(target, 'plugin.json')),
      shimsWritten: existsSync(join(binDir, 'gempt-wait.cmd')),
    });
    return { error: Object.assign(new Error('icacls unavailable'), { code: 'ENOENT' }) };
  });
  syncBuiltinESMExports();
  try {
    const env = { ...f.env, ANTIGRAVITY_CSRF_TOKEN: '' };
    const installed = await installWindowsAgy({ target, binDir, env });
    assert.equal(installed.installed, true);
    assert.equal(installed.shims.length, 3);
    assert.deepEqual(installed.cli_shims, ['gempt', 'gempt.cmd'].map((name) => join(binDir, name)));
    assert.deepEqual(calls.map(({ args }) => args), [
      [installed.plugin_dir, '/reset', '/T', '/C', '/Q'],
      ...[...installed.shims, ...installed.cli_shims].map((path) => [path, '/reset', '/C', '/Q']),
    ]);
    assert.deepEqual(calls.map(({ shimsWritten }) => shimsWritten), [false, true, true, true, true, true]);
    assert.ok(calls.every(({ command, exists, pluginPlaced, options }) =>
      command === 'icacls.exe' && exists && pluginPlaced && options.windowsHide === true));

    if (process.platform !== 'win32') {
      calls.length = 0;
      await installAgy({ target, binDir, env });
      assert.deepEqual(calls, []);
    }
  } finally {
    mock.mock.restore();
    syncBuiltinESMExports();
  }
});

test('Node CLI discovers, updates tiers, prints launchers and installs to an isolated home', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  const gateway = JSON.parse(readFileSync(f.configPath, 'utf8')).magpie.base_url;
  rmSync(f.configPath);
  const env = { ...f.env, HOME: f.root, USERPROFILE: f.root, MAGPIE_ADDR: gateway,
    ANTIGRAVITY_CSRF_TOKEN: '' };
  const cli = async (...args) => JSON.parse((await execFileAsync(process.execPath, [cliPath, ...args], {
    env, cwd: f.root, timeout: 15_000, windowsHide: true,
  })).stdout);
  const discovery = await cli('discover');
  assert.equal(discovery.config.path, f.configPath);
  assert.equal(typeof discovery.codex.found, 'boolean');
  const preview = await cli('init', '--non-interactive', '--sandbox', 'read-only',
    '--utility-model', 'fixture/utility', '--dry-run');
  assert.equal(preview.saved, false);
  assert.equal(preview.config.codex.default_sandbox, 'read-only');
  assert.equal(preview.config.utility.model, 'fixture/utility');
  assert.ok(!existsSync(f.configPath));
  await cli('init', '--non-interactive');
  assert.equal((await cli('tier')).default_tier, 'sonnet');
  assert.equal((await cli('tier', 'opus')).default_tier, 'opus');
  assert.equal((await cli('tier')).default_tier, 'opus');
  assert.equal((await cli('sandbox', 'read-only')).default_sandbox, 'read-only');
  assert.equal((await cli('effort', 'auto')).auto_effort, true);
  const id = 'a'.repeat(32);
  state(f, id, { state: 'finished' });
  assert.equal((await cli('wait', '--all', id)).run_id, id);
  assert.equal((await cli('status', '--run-id', id, '--wait')).state, 'finished');
  assert.match((await cli('print-mcp')).mcpServers.gempt.args[0], /core[/\\]mcp-server\.mjs$/);
  const target = join(f.root, 'cli plugin');
  const installed = await cli('install-agy', '--target', target);
  assert.equal(installed.plugin_dir, target);
  assert.ok(!existsSync(join(target, 'work_split_hook.py')));
  assert.ok(existsSync(join(f.root, 'bin', 'gempt-wait.mjs')));
  assert.deepEqual(await cli('print-mcp', '--installed', '--target', target), installed.mcp_config);
});

test('legacy routing fields do not affect effort settings or custom tier model IDs', (t) => {
  const f = fixture(t);
  const sonnet = {
    consult_model: 'group/custom-consult',
    exec_model: 'group/custom-exec',
    magpie_group_members: ['codex/gpt-6.2-astra'],
  };
  mkdirSync(dirname(f.configPath), { recursive: true });
  writeFileSync(f.configPath, JSON.stringify({
    routing_policy: { magpie_group_strategy: 'order' },
    tiers: { sonnet },
  }));
  assert.equal(setEffortSettings({ tier: 'sonnet', tierEffort: 'low' }, { configPath: f.configPath }).ok, true);
  const saved = JSON.parse(readFileSync(f.configPath, 'utf8'));
  const tier = buildTierView('sonnet', saved.tiers.sonnet, 'sonnet');
  assert.equal(tier.consult_model, sonnet.consult_model);
  assert.equal(tier.exec_model, sonnet.exec_model);
  assert.equal(tier.reasoning_effort, 'low');
  assert.equal(Object.hasOwn(tier, 'magpie_group_members'), false);
});

test('portable Antigravity bundle validates and a shim failure reports a partial install', async (t) => {
  const f = fixture(t);
  const bundleDir = join(f.root, 'bundle');
  stagePluginBundle(bundleDir, { portable: true });
  await validatePluginBundle(bundleDir, resolveNodeExecutable(), f.env, { portable: true });
  const launcher = join(bundleDir, 'runtime', 'core', 'assets', 'mcp-app', 'launcher.html');
  rmSync(launcher);
  await assert.rejects(validatePluginBundle(bundleDir, resolveNodeExecutable(), f.env, { portable: true }),
    /Missing or empty Antigravity plugin asset: .*launcher\.html/);
  copyFileSync(launcherUrl, launcher);
  const schemas = join(bundleDir, 'runtime', 'core', 'tool-schemas.mjs');
  const originalSchemas = readFileSync(schemas, 'utf8');
  writeFileSync(schemas, originalSchemas + '\nTOOLS[0]._meta["com.google.antigravity/ui"] = { entrypoints: [{ type: "aux_pane" }] };\n');
  await assert.rejects(validatePluginBundle(bundleDir, resolveNodeExecutable(), f.env, { portable: true }),
    /only inline launcher metadata/);
  writeFileSync(schemas, originalSchemas);
  writeFileSync(join(bundleDir, 'rules', 'AGENTS.md'), ' \n');
  await assert.rejects(validatePluginBundle(bundleDir, resolveNodeExecutable(), f.env, { portable: true }),
    /Empty Antigravity plugin rules text/);
  const manifest = JSON.parse(readFileSync(join(bundleDir, 'plugin.json'), 'utf8'));
  assert.equal(manifest.logo, 'assets/logo.svg');
  assert.equal(typeof manifest.author?.name, 'string');
  assert.ok(manifest.author.name.trim());
  for (const name of ['assets/logo.svg', 'THIRD_PARTY_LICENSES.txt', 'README.md']) {
    assert.ok(readFileSync(join(bundleDir, name), 'utf8').trim(), name);
  }

  const target = join(f.root, 'installed-plugin');
  const binDir = join(f.root, 'bin-file');
  writeFileSync(binDir, 'keep this file');
  const result = await installAgy({ target, binDir, env: f.env });
  assert.equal(result.plugin_installed, true);
  assert.equal(result.shim_installed, false);
  assert.equal(result.installed, false);
  assert.ok(result.shim_error);
  assert.ok(result.cli_shim_error);
  assert.ok(result.warnings.some((warning) => warning.includes(result.shim_error)));
  assert.equal(JSON.parse(readFileSync(join(target, 'plugin.json'), 'utf8')).name, 'gempt');
  assert.equal(readFileSync(binDir, 'utf8'), 'keep this file');
});

test('Antigravity installer stages assets from an installed bundle', async (t) => {
  const f = fixture(t);
  const bundle = join(f.root, 'bundle');
  stagePluginBundle(bundle, { portable: true });
  const template = join('templates', 'bundle-only.json');
  writeFileSync(join(bundle, template), '{}');
  const runtime = join(bundle, 'runtime');
  const installer = await import(pathToFileURL(join(runtime, 'core', 'agy-install.mjs')).href);
  const restaged = join(f.root, 'restaged');
  installer.stagePluginBundle(restaged, { portable: true });
  assert.equal(readFileSync(join(restaged, template), 'utf8'), '{}');
  await validatePluginBundle(restaged, process.execPath, f.env, { portable: true });
});

test('packaged native sidecar serves shared assets and authenticates scoped APIs with the Antigravity SDK', async (t) => {
  const sdk = join(homedir(), '.gemini', 'antigravity', 'lib', 'node_modules', 'sidecar_sdk');
  if (!existsSync(join(sdk, 'index.mjs'))) return t.skip('Antigravity Sidecar SDK is unavailable');
  const f = fixture(t);
  const bundle = join(f.root, 'bundle');
  stagePluginBundle(bundle, { portable: true });
  const alternate = join(f.root, 'another workspace');
  const active = 'active-conversation';
  mkdirSync(alternate);
  const id = '1'.repeat(32);
  const otherWorkspace = '2'.repeat(32);
  const otherConversation = '3'.repeat(32);
  state(f, id, { state: 'finished', host_conversation_id: active });
  state(f, otherWorkspace, { state: 'finished', host_conversation_id: active, cwd: alternate });
  state(f, otherConversation, { state: 'finished', host_conversation_id: 'other-conversation' });
  await updateHostContext('other-conversation', [f.cwd], f);
  await ensureSwarm({ swarmId: 'sidecar-board', hostConversationId: active, cwd: f.cwd, ...f });
  await upsertWorker('sidecar-board', { name: 'worker', state: 'waiting', source_cwd: f.cwd }, f);
  const shot = join(f.cwd, 'sidecar.png');
  const png = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  writeFileSync(shot, png);
  await postBoardMessage('sidecar-board', { from: 'worker', text: `Screenshot: ${shot} | Worker result` }, f);
  mkdirSync(dirname(f.configPath), { recursive: true });
  writeFileSync(f.configPath, '{}');
  const token = 'fixture-sidecar-token';
  const cwd = join(bundle, 'sidecars', 'gempt_live');
  const child = spawn(process.execPath, ['--import', pathToFileURL(join(sdk, 'register_loader.js')).href, 'main.mjs'], {
    cwd, env: { ...f.env, ANTIGRAVITY_LS_ADDRESS: '127.0.0.1:0', ANTIGRAVITY_CSRF_TOKEN: 'fixture',
      ANTIGRAVITY_CONVERSATION_ID: active, ANTIGRAVITY_SIDECAR_WEB_PORT: '0', ANTIGRAVITY_SIDECAR_UI_TOKEN: token },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const exited = once(child, 'exit');
  try {
    const scanner = createInterface({ input: child.stdout });
    const [line] = await once(scanner, 'line', { signal: AbortSignal.timeout(5000) });
    const base = 'http://' + line.match(/listening on (127[.]0[.]0[.]1:[0-9]+)/)[1];
    const html = await fetch(base).then((response) => response.text());
    assert.match(html, /loadScript\('gempt-preload\.js'\)/);
    assert.match(html, /loadScript\('app\.js',/);
    assert.doesNotMatch(html, /<script type="module"/);
    assert.equal(await fetch(base + '/index.html').then((response) => response.text()), html);
    assert.match(html, /id="view-console"/);
    const preload = await fetch(base + '/gempt-preload.js');
    assert.equal(preload.status, 200);
    const preloadSource = await preload.text();
    assert.match(preloadSource, /['"]\.\/_sidecar\/get-conversation-metadata/);
    assert.match(preloadSource, /const resolveProjectId = \(\) => projectIdPromise \?\?=/);
    const css = await fetch(base + '/styles.css');
    assert.match(css.headers.get('content-type'), /^text\/css/);
    assert.match(await css.text(), /var\(--background/);
    const js = await fetch(base + '/app.js');
    assert.match(js.headers.get('content-type'), /javascript/);
    assert.doesNotMatch(await js.text(), /@modelcontextprotocol|Invalid Zod|@cfworker/);
    assert.equal((await fetch(base + '/missing.js')).status, 404);

    const call = (path, payload = {}, key = token, method = 'POST') => fetch(base + '/api', {
      method, headers: { 'content-type': 'application/json', ...(key ? { 'x-sidecar-token': key } : {}) },
      body: JSON.stringify({ path: './api/' + path, ...payload }),
    });
    for (const method of ['POST', 'PUT', 'DELETE']) {
      assert.equal((await call('settings', { body: { codex_timeout_sec: 900 } }, '', method)).status, 401);
    }
    assert.equal((await call('summary', {}, 'bad-token')).status, 401);
    assert.equal((await fetch(base + '/api', { method: 'POST', headers: { origin: 'null' },
      body: JSON.stringify({ path: './api/summary' }),
    })).headers.get('access-control-allow-origin'), null);
    const opaqueSummary = await fetch(base + '/api?token=' + token, { method: 'POST', headers: { origin: 'null' },
      body: JSON.stringify({ path: './api/summary' }),
    });
    assert.equal(opaqueSummary.status, 200);
    assert.equal(opaqueSummary.headers.get('access-control-allow-origin'), 'null');
    assert.ok((await opaqueSummary.json()).routing);
    assert.equal((await fetch(base + '/api?token=' + token + '&path=./api/summary', { method: 'POST' })).status, 200);
    assert.equal((await fetch(base + '/api')).status, 405);
    const workspaceUris = [pathToFileURL(f.cwd).href];
    const swarm = await call('swarm/snapshot?tail=1&omitRaw=1', { workspaceUris }).then((response) => response.json());
    assert.equal(swarm.swarm_id, 'sidecar-board');
    assert.equal(swarm.self, 'coordinator');
    assert.equal(swarm.frames.length, 1);
    const frameRequest = swarm.frames[0].frameUrl.slice('./api/'.length);
    assert.equal((await call(frameRequest, { workspaceUris }, 'bad-token')).status, 401);
    assert.equal((await call(frameRequest, { workspaceUris }).then((response) => response.json())).data_url,
      'data:image/png;base64,' + png.toString('base64'));
    const list = await call('live/runs', { workspaceUris }).then((response) => response.json());
    assert.equal(list.active_conversation_id, active);
    assert.deepEqual(list.runs.map((run) => run.run_id), [id]);
    const all = await call('live/runs?scope=all', { workspaceUris }).then((response) => response.json());
    assert.deepEqual(new Set(all.runs.map((run) => run.run_id)), new Set([id, otherConversation]));
    const multi = await call('live/runs', { workspaceUris: [...workspaceUris, pathToFileURL(alternate).href] })
      .then((response) => response.json());
    assert.deepEqual(new Set(multi.runs.map((run) => run.run_id)), new Set([id, otherWorkspace]));
    assert.equal((await call('live/stream?run_id=' + otherWorkspace, { workspaceUris })).status, 403);
    assert.equal((await call('requests?format=csv')).headers.get('content-type'), 'text/csv; charset=utf-8');
    assert.equal((await call('settings', { body: { codex_timeout_sec: 900 } })).status, 200);
    assert.equal(JSON.parse(readFileSync(f.configPath, 'utf8')).codex.timeout_sec, 900);
  } finally {
    child.kill();
    await exited;
  }
});

function git(cwd, ...args) {
  const result = spawnSync('git', ['-C', cwd, ...args], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  return result.stdout.trim();
}

function initRepo(cwd) {
  const subdir = join(cwd, 'project');
  mkdirSync(subdir);
  writeFileSync(join(subdir, 'readme.txt'), 'tracked\n');
  git(cwd, 'init', '-q');
  git(cwd, 'add', '.');
  git(cwd, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture');
  return subdir;
}

function state(f, id, fields) {
  const path = join(runDirectory(id, f.sessionsDir), 'state.json');
  saveRun(path, { run_id: id, state: 'running', session_id: `exec-${id.slice(0, 1)}`, cwd: f.cwd, ...fields });
}

async function appFixture(t, f, controls = {}) {
  const packets = [];
  const turns = new Map();
  const sockets = new Set();
  const openThread = threadFixture();
  let turnCount = 0;
  const server = createServer((req, res) => {
    res.setHeader('content-type', 'application/json');
    if (controls.registrations && req.url === '/api/internal/cache-key') {
      assert.equal(req.headers['x-gempt-ipc-token'], 'registration-token');
      let data = '';
      req.on('data', (chunk) => { data += chunk; });
      req.on('end', () => {
        controls.registrations.push(JSON.parse(data));
        res.end('{}');
      });
      return;
    }
    if (req.url === '/v1/models') res.end(JSON.stringify({ data: [{ id: 'codex/gpt-6.2-sol' }] }));
    else if (req.url === '/v1/responses') res.end(JSON.stringify({ model: 'codex/gpt-6.2-sol', output_text: 'Consult answer.' }));
    else { res.statusCode = 404; res.end('{}'); }
  });
  server.on('upgrade', (req, socket) => {
    if (req.headers.authorization !== `Bearer ${controls.token || 'test-token'}`) {
      socket.end('HTTP/1.1 401 Unauthorized\r\n\r\n');
      return;
    }
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    let activeTurn;
    socket.on('error', (err) => { if (err.code !== 'ECONNRESET') throw err; });
    serveAppServerSocket(req, socket, (packet, send) => {
      packets.push(packet);
      const { method, id, params } = packet;
      if (method === 'initialized') return;
      let result = {};
      if (['thread/start', 'thread/resume', 'thread/fork'].includes(method)) {
        if (method === 'thread/fork' && (controls.rejectFork || controls.rejectForkConfig && params.config)) {
          return send({ id, error: { code: -32601, message: 'Unknown method thread/fork' } });
        }
        result = openThread(method, params);
        if (method === 'thread/resume' && controls.completeOnRejoin) {
          setImmediate(() => send({ method: 'turn/completed', params: { threadId: params.threadId,
            turn: { id: 'turn-1', status: 'completed', items: [{ type: 'agentMessage', id: 'final', text: 'after reconnect' }] } } }));
        }
      } else if (method === 'thread/read' && controls.readThread) {
        assert.equal(params.includeTurns, true);
        result = { thread: controls.readThread(params.threadId) };
      } else if (method === 'turn/start') {
        if (controls.rejectStart) {
          controls.beforeReject?.();
          return send({ id, error: { code: -32602, message: 'Rejected turn/start' } });
        }
        if (controls.registrations) assert.ok(controls.registrations.some((r) => r.threadId === params.threadId));
        const turnId = activeTurn = `turn-${++turnCount}`;
        const threadId = params.threadId;
        const event = (method, params) => send({ method, params: { threadId, turnId, ...params } });
        turns.set(turnId, (status) => {
          if (status === 'completed') event('item/completed', { item: { id: 'answer', type: 'agentMessage', text: 'Prior finding: verify the result.' } });
          event('turn/completed', { turn: { id: turnId, status } });
          turns.delete(turnId);
        });
        if (params.input[0].text.includes('Ask before turn acknowledgement')) {
          controls.ask = () => send({ id: 'early-ask-rpc', method: 'item/tool/call', params: {
            threadId, turnId, callId: 'early_ask', tool: 'ask_gemini', arguments: { question: 'Proceed?' },
          } });
          return;
        }
        event('turn/started', { turn: { id: turnId, status: 'inProgress' } });
        if (controls.changedFile) event('item/completed', { item: {
          type: 'fileChange', id: 'edit', changes: [{ path: controls.changedFile }],
        } });
        for (const tokenUsage of controls.usage || []) event('thread/tokenUsage/updated', { tokenUsage });
        if (params.input[0].text.includes('Disconnect the turn socket')) {
          controls.beforeDisconnect?.();
          socket.end();
          return;
        }
        send({ id, result: { turn: { id: turnId } } });
        if (params.input[0].text.includes('Request a cross-check')) {
          send({ id: 'ask-rpc', method: 'item/tool/call', params: { threadId, turnId, callId: 'ask_1',
            tool: 'ask_gemini', arguments: { reason: 'decision', question: 'Proceed?' } } });
          if (controls.parallelAsk) send({ id: 'ask-second', method: 'item/tool/call', params: {
            threadId, turnId, callId: 'ask_2', tool: 'ask_gemini', arguments: { reason: 'decision', question: 'Second question?' },
          } });
        } else if (!params.input[0].text.includes('Hold this turn')) turns.get(turnId)('completed');
        return;
      } else if (method === 'turn/interrupt') {
        if (controls.interruptError) return send({ id, error: { code: -32000, message: controls.interruptError } });
        if (turns.has(params.turnId)) turns.get(params.turnId)('interrupted');
        else send({ method: 'turn/completed', params: { threadId: params.threadId, turn: { id: params.turnId, status: 'interrupted' } } });
      } else if (id === 'ask-rpc') {
        turns.get(activeTurn)?.('completed');
        return;
      } else if (id === 'ask-second') {
        return;
      }
      send({ id, result });
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => {
    for (const socket of sockets) socket.destroy();
    return new Promise((resolve) => server.close(resolve));
  });
  const address = `127.0.0.1:${server.address().port}`;
  const gateway = `http://${address}`;
  mkdirSync(join(f.root, 'config'), { recursive: true });
  writeFileSync(f.configPath, JSON.stringify({ version: 1, magpie: { base_url: gateway },
    codex: { default_sandbox: 'workspace-write' }, routing_policy: { default_tier: 'sonnet', failover_mode: 'subscription_only' },
    tiers: { sonnet: { consult_model: 'codex/gpt-6.2-sol', exec_model: 'codex/gpt-6.2-sol', reasoning_effort: 'medium' } } }));
  f.env.GEMPT_APP_SERVER_STATE = join(f.root, 'app-server.json');
  const port = server.address().port;
  if (controls.registrations) writeFileSync(join(dirname(f.configPath), `page-server-${port}.ipc`),
    JSON.stringify({ port, token: 'registration-token' }));
  writeFileSync(f.env.GEMPT_APP_SERVER_STATE, JSON.stringify({ status: 'running', ws_url: `ws://${address}`,
    ...(controls.registrations ? { proxy_port: port } : {}),
    auth_header: 'Authorization', auth_token: controls.token || 'test-token', gateway_url: gateway }));
  return packets;
}

async function until(probe) {
  const end = performance.now() + 5000;
  while (performance.now() < end) {
    const value = await probe();
    if (value) return value;
    await sleep(20);
  }
  assert.fail('Timed out waiting for fixture state.');
}

test('gempt run and ask accept shell input and share execution and consult sessions with MCP', async (t) => {
  const f = fixture(t);
  const repoSubdir = initRepo(f.cwd);
  const workerCwd = join(f.root, 'native swarm worktree');
  git(f.cwd, 'worktree', 'add', '--detach', workerCwd, 'HEAD');
  const packets = await appFixture(t, f);
  const baseEnv = { ...f.env, GEMPT_WORKSPACE_ROOT: f.root, GEMPT_GPT_RUN_SYNC_SEC: '0' };
  const cli = (args, input = '', env = baseEnv) => {
    const pending = execFileAsync(process.execPath, [cliPath, ...args], { cwd: workerCwd, env, timeout: 15_000 });
    pending.child.stdin.end(input);
    return pending.then(({ stdout }) => JSON.parse(stdout));
  };
  const run = await cli(['run', '--goal', '-', '--access', 'inspect', '--workspace', 'inherit', '--tier', 'sonnet',
    '--family', 'hydrogen', '--host', 'coordinator', '--title', 'inspect worker', '--timeout', '90'], '  Inspect the native worktree. \n');
  assert.equal(run.state, 'queued');
  assert.equal(run.cwd, workerCwd);
  assert.equal(run.worktree_path, null);
  assert.equal(run.tier, 'sonnet');
  assert.match(run.wait_command, new RegExp(run.run_id + '$'));
  assert.equal(run.goal, undefined);
  const done = await waitForRun(run.run_id, { ...f, timeoutSec: 5 });
  assert.equal(done.state, 'finished', done.error);
  assert.equal(done.session_id, run.session_id);
  assert.equal(done.host_conversation_id, 'coordinator');
  assert.equal(done.title, 'inspect worker');
  assert.equal(done.sandbox, 'read-only');
  assert.equal(done.goal, 'Inspect the native worktree.');
  assert.equal(git(f.cwd, 'worktree', 'list', '--porcelain').match(/^worktree /gm).length, 2);

  const goalFile = join(f.root, 'brief.txt');
  writeFileSync(goalFile, 'Follow up on the inspection.');
  const follow = await cli(['run', '--goal-file', goalFile, '--cwd', workerCwd, '--access', 'inspect',
    '--session', run.session_id], '', { ...baseEnv, GEMPT_GPT_RUN_SYNC_SEC: '5' });
  assert.equal(follow.state, 'finished');
  assert.equal(follow.session_id, run.session_id);
  assert.equal(follow.cwd, workerCwd);
  assert.match(follow.text, /Prior finding/);
  assert.match(packets.filter(({ method }) => method === 'turn/start').at(-1).params.input[0].text, /Follow up on the inspection/);

  writeFileSync(join(repoSubdir, 'second.txt'), 'second attachment');
  const askArgs = ['ask', '--cwd', repoSubdir, '--tier', 'sonnet', '--host', 'coordinator', '--file', 'readme.txt', '--file', 'second.txt'];
  const answer = await cli([...askArgs, '--max-output-tokens', '7'], 'Summarize these files.');
  assert.equal(run.execution_timeout_sec, 90);
  for (const flag of ['0', 'none', 'unlimited']) {
    const unlimited = await cli(['run', '--goal', 'Inspect', '--access', 'inspect', '--timeout', flag]);
    assert.equal(unlimited.execution_timeout_sec, null);
    assert.equal(unlimited.deadline, null);
    const finished = await waitForRun(unlimited.run_id, { ...f, timeoutSec: 5 });
    assert.equal(finished.state, 'finished', finished.error);
    assert.equal(finished.deadline, null);
  }
  assert.match(answer.text, /Consult answer/);
  const consulted = JSON.parse(readFileSync(join(f.sessionsDir, answer.session_id + '.json'), 'utf8'));
  assert.equal(consulted.host_conversation_id, 'coordinator');
  const prompt = consulted.turns.find(({ role }) => role === 'user').content;
  assert.match(prompt, /tracked/);
  assert.match(prompt, /second attachment/);
  const continuation = await cli(['ask', '--goal-file', goalFile, '--cwd', repoSubdir, '--session', answer.session_id]);
  assert.equal(continuation.session_id, answer.session_id);
  await assert.rejects(cli([...askArgs, '--file', '../outside.txt'], 'Check attachments.'), /outside workspace/);

  for (const [args, input, error] of [
    [['run'], ' \n', /goal is required/],
    [['ask', '--goal', '-'], '', /goal is required/],
    [['ask', '--goal', 'x', '--goal-file', goalFile], '', /Choose --goal or --goal-file/],
    [['run', '--access', 'admin'], '', /Invalid --access/],
    [['run', '--workspace', 'other'], '', /Invalid --workspace/],
    [['run', '--access', 'inspect', '--workspace', 'share', '--goal', 'x'], '', /not allowed with read-only/],
    [['run', '--tier', 'chore'], '', /Invalid --tier/],
    [['ask', '--tier', 'unknown'], '', /Invalid --tier/],
    [['ask', '--family', 'unknown'], '', /Invalid --family/],
    [['run', '--timeout', 'Infinity'], '', /Invalid timeout_sec/],
  ]) await assert.rejects(cli(args, input), error);
});

test('host context reclaims a dead PID lock, restores the main conversation and tracks subagents', async (t) => {
  const { cwd, configPath } = fixture(t);
  const opts = { configPath };
  const lock = join(dirname(configPath), 'host-context.lock');
  const exited = spawnSync(process.execPath, ['-e', 'process.exit(0)']);
  assert.equal(exited.status, 0);
  mkdirSync(dirname(configPath));
  writeFileSync(lock, `${exited.pid}:${Date.now()}\n`);
  assert.equal(basename(contextPath(configPath)), 'host-context.json');
  assert.deepEqual(readHostContext(configPath), {});
  await updateHostContext('main', [cwd, 1], { ...opts, geminiFamily: ' pro ' });
  assert.equal(readHostContext(configPath).conversation_id, 'main');
  assert.ok(!existsSync(lock));
  await updateHostContext('child', ['other'], opts);
  await updateHostContext('child', ['other'], { ...opts, parentId: 'main' });
  assert.equal(readHostContext(configPath).conversation_id, 'main');
  assert.deepEqual(readHostContext(configPath).workspace_paths, [cwd]);
  await Promise.all(['worker-z', 'worker-a'].map((id) => updateHostContext(id, [], { ...opts, parentId: 'main' })));
  const before = readHostContext(configPath);
  assert.deepEqual(before.subagent_conversations, ['child', 'worker-a', 'worker-z']);
  assert.equal(before.gemini_family, 'pro');
  assert.ok(before.updated_at > 0);
  await updateHostContext('child', [], opts);
  assert.deepEqual(readHostContext(configPath), before);
});

test('lock retries while an owner is alive and reclaims a malformed legacy record', async (t) => {
  const { root } = fixture(t);
  const lock = join(root, 'state.lock');
  writeFileSync(lock, 'legacy\n');
  const events = [];
  const first = withLock(lock, async () => {
    assert.match(readFileSync(lock, 'utf8'), new RegExp(`^${process.pid}:\\d+\\n$`));
    events.push('first enters');
    await sleep(100);
    events.push('first leaves');
  });
  const second = withLock(lock, () => events.push('second enters'));
  await Promise.all([first, second]);
  assert.deepEqual(events, ['first enters', 'first leaves', 'second enters']);
  assert.ok(!existsSync(lock));
});

test('live PID lock is not stolen past staleMs and replaced locks are preserved on release', async (t) => {
  const { root } = fixture(t);
  const lock = join(root, 'state.lock');
  assert.equal(DEFAULT_LOCK_STALE_MS, 300_000);
  assert.equal(lockStaleMsForBudget(180), 300_000);
  assert.equal(lockStaleMsForBudget(600), 660_000);
  let releaseFirst;
  const first = withLock(lock, () => new Promise((resolve) => { releaseFirst = resolve; }));
  const expired = new Date(Date.now() - 10_000);
  utimesSync(lock, expired, expired);
  try {
    await assert.rejects(
      withLock(lock, () => {}, { attempts: 3, delayMs: 10, staleMs: 5_000 }),
      { code: 'EEXIST' },
    );
    const exited = spawnSync(process.execPath, ['-e', 'process.exit(0)']);
    assert.equal(exited.status, 0);
    rmSync(lock);
    writeFileSync(lock, `${exited.pid}:${Date.now() - 10_000}\n`);
    await withLock(lock, async () => {
      const replacement = readFileSync(lock, 'utf8');
      releaseFirst();
      await first;
      assert.equal(readFileSync(lock, 'utf8'), replacement);
    }, { staleMs: 5_000 });
  } finally {
    releaseFirst();
    await first;
  }
  assert.ok(!existsSync(lock));
});

test('shared worktrees use detached Git HEAD, preserve subdirectories, reuse, and remove', (t) => {
  const { cwd, env } = fixture(t);
  const subdir = initRepo(cwd);
  const worktree = ensureShareWorktree(subdir, 'session-a', env);
  assert.equal(worktree.worktree_root, join(worktreesRoot(env), 'session-a'));
  assert.equal(worktree.cwd, join(worktree.worktree_root, 'project'));
  assert.equal(worktree.source_repo, cwd);
  assert.equal(worktree.source_cwd, subdir);
  assert.equal(git(worktree.cwd, 'rev-parse', '--abbrev-ref', 'HEAD'), 'HEAD');
  assert.equal(git(worktree.cwd, 'rev-parse', 'HEAD'), git(cwd, 'rev-parse', 'HEAD'));
  writeFileSync(join(worktree.cwd, 'new.txt'), 'work in progress');
  assert.deepEqual(ensureShareWorktree(subdir, 'session-a', env), worktree);
  assert.equal(readFileSync(join(worktree.cwd, 'new.txt'), 'utf8'), 'work in progress');
  assert.throws(() => ensureShareWorktree(cwd, '../unsafe', env), /Invalid session_id/);
  assert.equal(removeShareWorktree(cwd, cwd, env), false);
  rmSync(join(worktree.cwd, 'new.txt'));
  assert.equal(removeShareWorktree(cwd, worktree.worktree_root, env), true);
  assert.equal(existsSync(worktree.worktree_root), false);
  assert.doesNotMatch(git(cwd, 'worktree', 'list'), /session-a/);
});

test('shared worktree entry points reject inspect and cleanup protects foreign and dirty worktrees', async (t) => {
  const f = fixture(t);
  const subdir = initRepo(f.cwd);
  const request = { goal: 'Inspect the project', access: 'inspect', workspace: 'share', cwd: subdir };
  const readOnlyError = /workspace='share' is not allowed with read-only access/;
  const originalWorktrees = git(f.cwd, 'worktree', 'list', '--porcelain');
  await assert.rejects(dispatchRun(request, f), readOnlyError);
  assert.equal(existsSync(worktreesRoot(f.env)), false);
  await assert.rejects(handleTool('gpt_run', request, f.env), readOnlyError);
  assert.equal(existsSync(worktreesRoot(f.env)), false);
  assert.equal(git(f.cwd, 'worktree', 'list', '--porcelain'), originalWorktrees);

  const otherRepo = join(f.root, 'other-repo');
  mkdirSync(otherRepo);
  initRepo(otherRepo);
  const foreign = ensureShareWorktree(otherRepo, 'foreign', f.env).worktree_root;
  assert.throws(() => assertShareWorktree(f.cwd, foreign, f.env), /does not belong to the requested Git repository/);

  const dirty = ensureShareWorktree(subdir, 'dirty', f.env);
  const edited = join(dirty.cwd, 'readme.txt');
  writeFileSync(edited, 'uncommitted edit\n');
  assert.equal(removeShareWorktree(f.cwd, dirty.worktree_root, f.env), false);

  const clean = ensureShareWorktree(subdir, 'clean', f.env);
  writeFileSync(join(clean.cwd, 'merged.txt'), 'merged change\n');
  git(clean.cwd, 'add', 'merged.txt');
  git(clean.cwd, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
    '-c', 'commit.gpgsign=false', 'commit', '-qm', 'worktree change');
  const commit = git(clean.cwd, 'rev-parse', 'HEAD');
  assert.equal(removeShareWorktree(f.cwd, clean.worktree_root, f.env), false);
  git(f.cwd, 'merge', '--ff-only', commit);
  assert.deepEqual(pruneShareWorktrees(f.cwd, { env: f.env, maxAgeMs: 0 }),
    { removed: [clean.worktree_root], kept: [dirty.worktree_root] });
  assert.equal(existsSync(clean.worktree_root), false);
  assert.doesNotMatch(git(f.cwd, 'worktree', 'list', '--porcelain'), /\/clean(?:\r?\n|$)/);
  assert.equal(readFileSync(edited, 'utf8'), 'uncommitted edit\n');
  assert.equal(assertShareWorktree(otherRepo, foreign, f.env), foreign);
});

test('external linked worktrees map nested paths and require matching Git metadata', (t) => {
  const f = fixture(t);
  const mainSubdir = initRepo(f.cwd);
  const tree = join(f.root, 'linked');
  git(f.cwd, 'worktree', 'add', '--detach', tree);
  const branchOnly = join(tree, 'branch-only');
  mkdirSync(branchOnly);
  assert.equal(resolveExternalWorktreeSourceCwd(f.cwd), null);
  assert.equal(resolveExternalWorktreeSourceCwd(tree), f.cwd);
  assert.equal(resolveExternalWorktreeSourceCwd(join(tree, 'project')), mainSubdir);
  assert.equal(existsSync(join(f.cwd, 'branch-only')), false);
  assert.equal(resolveExternalWorktreeSourceCwd(branchOnly), join(f.cwd, 'branch-only'));
  assert.equal(resolveExternalWorktreeSourceCwd(join(tree, 'missing')), null);

  const copied = join(f.root, 'copied');
  mkdirSync(copied);
  copyFileSync(join(tree, '.git'), join(copied, '.git'));
  assert.equal(resolveExternalWorktreeSourceCwd(copied), null);
  const childRepo = join(f.root, 'child');
  mkdirSync(childRepo);
  initRepo(childRepo);
  git(f.cwd, '-c', 'protocol.file.allow=always', 'submodule', 'add', childRepo, 'submodule');
  assert.equal(resolveExternalWorktreeSourceCwd(join(f.cwd, 'submodule')), null);
  const bare = join(f.root, 'bare.git');
  git(f.root, 'clone', '--bare', childRepo, bare);
  const bareTree = join(f.root, 'bare-linked');
  git(bare, 'worktree', 'add', '--detach', bareTree);
  assert.equal(resolveExternalWorktreeSourceCwd(bareTree), null);

  const roots = [f.cwd];
  assert.deepEqual(workspaceFileRoots({ cwd: f.cwd, source_cwd: copied }, roots), roots);
  assert.deepEqual(workspaceFileRoots({ cwd: copied, source_cwd: f.cwd }, roots), roots);
  const share = { cwd: copied, source_cwd: f.cwd, worktree_path: copied };
  assert.deepEqual(workspaceFileRoots(share, roots), roots);
  assert.deepEqual(workspaceFileRoots({ ...share, worktree_path: tree }, roots), [...roots, tree]);
  const managed = join(worktreesRoot(), basename(f.root));
  assert.deepEqual(workspaceFileRoots({ ...share, worktree_path: managed }, roots), [...roots, managed]);
  assert.deepEqual(workspaceFileRoots({ ...share, worktree_path: managed, source_cwd: childRepo }, roots), roots);
});

test('external linked worktree runs retain workspace visibility while file links require a current backlink', async (t) => {
  const f = fixture(t);
  initRepo(f.cwd);
  const tree = join(f.root, 'linked');
  git(f.cwd, 'worktree', 'add', '--detach', tree);
  const changedFile = 'project/readme.txt';
  await appFixture(t, f, { changedFile });
  f.env.GEMPT_WORKSPACE_ROOT = tree;
  const run = await dispatchRun({ goal: 'Request a cross-check', cwd: tree, workspace: 'inherit', background: true }, f);
  await until(() => readRun(run.run_id, f.sessionsDir).state === 'waiting_gemini');
  const disk = JSON.parse(readFileSync(join(runDirectory(run.run_id, f.sessionsDir), 'state.json')));
  assert.equal(disk.source_cwd, f.cwd);
  assert.equal(disk.worktree_path, null);
  assert.equal(disk.cwd, tree);
  const legacyId = 'a'.repeat(32);
  state(f, legacyId, { pid: process.pid, cwd: tree, source_cwd: null, progress: { touched_files: [changedFile] } });
  const ids = [run.run_id, legacyId].sort();
  const scoped = { ...f, workspaceRoots: [f.cwd] };
  const unrelated = { ...f, workspaceRoots: [join(f.root, 'other')] };
  const listed = await runLiveOperation('runs', { scope: 'all' }, scoped);
  assert.deepEqual(listed.runs.map((r) => r.run_id).sort(), ids);
  assert.ok(listed.runs.every((r) => r.workspace_name === basename(f.cwd)));
  assert.deepEqual(listActiveRoster({ ...f, root: f.cwd }).map((r) => r.run_id).sort(), ids);
  assert.deepEqual(listActiveRoster({ ...f, root: unrelated.workspaceRoots[0] }), []);
  assert.deepEqual((await runLiveOperation('runs', { scope: 'all' }, unrelated)).runs, []);
  for (const run_id of ids) {
    const record = workspaceRun(run_id, scoped);
    assert.ok(runWithinRoot(record, f.cwd));
    assert.equal(runWithinRoot(record, unrelated.workspaceRoots[0]), false);
    assert.throws(() => workspaceRun(run_id, unrelated), { status: 403 });
    const opened = await runLiveOperation('open', { run_id, scope: 'all' }, scoped);
    assert.equal(opened.run_id, run_id);
    const streamed = await runLiveOperation('stream', { run_id }, scoped);
    assert.equal(streamed.file_uris[changedFile], pathToFileURL(join(tree, changedFile)).href);
  }
  replyToolCall(run.run_id, 'ask_1', 'Confirmed.', f);
  await until(() => readRun(run.run_id, f.sessionsDir).state === 'finished');
  assert.deepEqual(pruneShareWorktrees(f.cwd, { env: f.env, maxAgeMs: 0 }), { removed: [], kept: [] });
  assert.equal(resolveExternalWorktreeSourceCwd(tree), f.cwd);
  const moved = join(f.root, 'moved');
  git(f.cwd, 'worktree', 'move', tree, moved);
  mkdirSync(join(tree, 'project'), { recursive: true });
  writeFileSync(join(tree, changedFile), 'replacement');
  copyFileSync(join(moved, '.git'), join(tree, '.git'));
  for (const run_id of ids) {
    const stream = await runLiveOperation('stream', { run_id }, scoped);
    assert.equal(stream.file_uris[changedFile], undefined);
    assert.equal(stream.restricted_files[changedFile], 'outside_workspace');
  }
});

test('background dispatch reuses a shared worktree and upgrades inspect to a fresh writable thread', async (t) => {
  const f = fixture(t);
  const subdir = initRepo(f.cwd);
  const packets = await appFixture(t, f);
  await updateHostContext('host-main', [f.cwd], f);
  const first = await dispatchRun({ goal: 'Read-only inspection', cwd: subdir, workspace: 'inherit',
    background: true, sandbox: 'read-only' }, f);
  assert.equal(first.state, 'queued');
  assert.equal(first.host_conversation_id, 'host-main');
  assert.equal(first.worktree_path, null);
  const done = await waitForRun(first.run_id, { ...f, timeoutSec: 5 });
  assert.equal(done.state, 'finished', done.error);
  assert.equal(done.sandbox, 'read-only');
  const second = await dispatchRun({ goal: 'Make the change', cwd: subdir, session_id: done.session_id,
    workspace: 'inherit', background: true }, f);
  assert.equal(second.worktree_path, null);
  const upgraded = await waitForRun(second.run_id, { ...f, timeoutSec: 5 });
  assert.equal(upgraded.state, 'finished', upgraded.error);
  assert.equal(upgraded.sandbox_upgraded, true);
  assert.equal(upgraded.result.sandbox_upgraded, true);
  assert.equal(upgraded.cwd, first.cwd);
  assert.notEqual(upgraded.codex_thread_id, done.codex_thread_id);
  const session = JSON.parse(readFileSync(join(f.sessionsDir, `${done.session_id}.json`), 'utf8'));
  assert.equal(session.sandbox_upgraded, true);
  assert.equal(session.sandbox, 'workspace-write');
  assert.equal(session.codex_thread_id, upgraded.codex_thread_id);
  assert.deepEqual(packets.filter(({ method }) => /^thread\/(start|resume)$/.test(method)).map(({ method, params }) =>
    [method, params.sandbox]), [['thread/start', 'read-only'], ['thread/start', 'workspace-write']]);
  const input = packets.filter(({ method }) => method === 'turn/start').at(-1).params;
  assert.deepEqual(input.sandboxPolicy, { type: 'workspaceWrite' });
  assert.match(input.input[0].text, /Carryover from the prior read-only Codex thread/);
  assert.match(input.input[0].text, /Prior finding: verify the result/);

  const sharedFirst = await dispatchRun({ goal: 'Edit in shared worktree', cwd: subdir, workspace: 'share',
    background: true }, f);
  assert.equal(sharedFirst.cwd, join(sharedFirst.worktree_path, 'project'));
  const sharedDone = await waitForRun(sharedFirst.run_id, { ...f, timeoutSec: 5 });
  assert.equal(sharedDone.state, 'finished', sharedDone.error);
  const sharedSecond = await dispatchRun({ goal: 'Follow up in shared worktree', cwd: subdir,
    session_id: sharedDone.session_id, workspace: 'inherit', background: true }, f);
  assert.equal(sharedSecond.worktree_path, sharedFirst.worktree_path);
  assert.equal((await waitForRun(sharedSecond.run_id, { ...f, timeoutSec: 5 })).state, 'finished');

  const priorId = 'exec-python-example';
  writeFileSync(join(f.sessionsDir, `${priorId}.json`), JSON.stringify({ mode: 'exec', session_id: priorId,
    cwd: subdir, sandbox: 'read-only', codex_thread_id: 'legacy-thread', turns: [{ goal: 'Inspect on Python', reply: 'Legacy finding.' }] }));
  await assert.rejects(dispatchRun({ goal: 'Exceeds host ceiling', cwd: subdir, session_id: priorId,
    sandbox: 'danger-full-access' }, { ...f, waitSec: 5 }), /cannot increase ceiling/);
  const config = JSON.parse(readFileSync(f.configPath, 'utf8'));
  config.codex.default_sandbox = 'danger-full-access';
  writeFileSync(f.configPath, JSON.stringify(config));
  const full = await dispatchRun({ goal: 'Resume with full access', cwd: subdir, session_id: priorId,
    sandbox: 'danger-full-access' }, { ...f, waitSec: 5 });
  assert.equal(full.sandbox_upgraded, true);
  assert.equal(full.sandbox, 'danger-full-access');
  assert.notEqual(full.codex_thread_id, 'legacy-thread');
  assert.match(packets.findLast(({ method }) => method === 'turn/start').params.input[0].text, /Legacy finding/);
});

test('steer and interrupt send turn identity over WebSocket and ask_gemini relays file replies', async (t) => {
  const f = fixture(t);
  const packets = await appFixture(t, f);
  const seed = await dispatchRun({ goal: 'Create a session to resume', cwd: f.cwd, background: true }, f);
  const ready = await waitForRun(seed.run_id, { ...f, timeoutSec: 5 });
  assert.equal(ready.state, 'finished', ready.error);
  const args = { goal: 'Hold this turn', cwd: f.cwd, session_id: seed.session_id, background: true };
  const competingId = process.platform === 'win32' ? seed.session_id.toUpperCase() : seed.session_id;
  const claims = await Promise.allSettled([dispatchRun(args, f), dispatchRun({ ...args, session_id: competingId }, f)]);
  assert.equal(claims.filter(({ status }) => status === 'fulfilled').length, 1);
  assert.equal(claims.find(({ status }) => status === 'rejected').reason.status, 409);
  const run = claims.find(({ status }) => status === 'fulfilled').value;
  const active = await until(() => {
    const record = readRun(run.run_id, f.sessionsDir);
    assert.notEqual(record.state, 'failed', record.error);
    return record.turn_id && record;
  });
  await assert.rejects(steerRun(run.run_id, ' ', f), /non-empty/);
  assert.equal((await steerRun(run.run_id, 'Check the logs.', f)).turn_id, active.turn_id);
  assert.deepEqual(packets.find(({ method }) => method === 'turn/steer').params, {
    threadId: active.codex_thread_id, expectedTurnId: active.turn_id, input: [{ type: 'text', text: 'Check the logs.' }],
  });
  assert.equal((await interruptRun(run.run_id, f)).ok, true);
  assert.deepEqual(packets.find(({ method }) => method === 'turn/interrupt').params, {
    threadId: active.codex_thread_id, turnId: active.turn_id,
  });
  assert.equal((await waitForRun(run.run_id, { ...f, timeoutSec: 5 })).state, 'interrupted');
  await assert.rejects(interruptRun(run.run_id, f), /no active Codex turn/);

  const asking = await dispatchRun({ goal: 'Request a cross-check', cwd: f.cwd, background: true }, f);
  const waiting = await waitForRun(asking.run_id, { ...f, timeoutSec: 5 });
  assert.equal(waiting.state, 'waiting_gemini', waiting.error);
  assert.equal(waiting.pending_tool_call.call_id, 'ask_1');
  assert.throws(() => replyToolCall(asking.run_id, '../bad', 'answer', f), /Invalid tool call_id/);
  assert.equal(replyToolCall(asking.run_id, 'ask_1', 'Confirmed.', f).ok, true);
  assert.equal(replyToolCall(asking.run_id, 'ask_1', 'Confirmed.', f).ok, true);
  assert.throws(() => replyToolCall(asking.run_id, 'ask_1', 'Conflicting answer.', f), /different reply|not waiting/);
  const finished = await until(() => {
    const record = readRun(asking.run_id, f.sessionsDir);
    assert.notEqual(record.state, 'failed', record.error);
    return record.state === 'finished' && record;
  });
  assert.equal(finished.pending_tool_call, null);
  assert.deepEqual(packets.find(({ id, method }) => id === 'ask-rpc' && !method).result,
    { success: true, contentItems: [{ type: 'inputText', text: 'Confirmed.' }] });
});

test('parallel ask_gemini calls accept explicit out-of-order replies and resume the live worker', async (t) => {
  const f = fixture(t);
  const packets = await appFixture(t, f, { parallelAsk: true });
  const run = await dispatchRun({ goal: 'Request a cross-check', cwd: f.cwd, background: true }, f);
  const pending = await until(() => readRun(run.run_id, f.sessionsDir).pending_tool_calls);
  const thread = packets.find((p) => p.method === 'thread/start').params;
  assert.match(thread.developerInstructions, /matching \.h\/\.hpp files.*final report/);
  assert.match(thread.dynamicTools.find((tool) => tool.name === 'ask_gemini').description, /high-risk or destructive.*ambiguity/);
  assert.deepEqual(pending.map((call) => call.call_id), ['ask_1', 'ask_2']);
  const answer = (callId, text) => jsonProcess(process.execPath,
    [cliPath, 'reply-tool', run.run_id, callId, '--text', text], { env: f.env });
  answer('ask_2', 'Second question answered first');
  const remaining = await until(() => {
    const record = readRun(run.run_id, f.sessionsDir);
    return !record.pending_tool_calls && packets.some((p) => p.id === 'ask-second' && p.result) && record;
  });
  assert.equal(remaining.state, 'waiting_gemini');
  assert.equal(remaining.pending_tool_call.call_id, 'ask_1');
  assert.equal(remaining.pending_tool_calls, undefined);
  answer('ask_1', 'First question answered second');
  await until(() => readRun(run.run_id, f.sessionsDir).state === 'finished');
  assert.deepEqual(packets.find(({ id, result }) => id === 'ask-second' && result).result,
    { success: true, contentItems: [{ type: 'inputText', text: 'Second question answered first' }] });
});

test('local turn errors interrupt, while transport loss probes without cancelling the turn', async (t) => {
  const f = fixture(t);
  const packets = await appFixture(t, f);
  const asking = await dispatchRun({ goal: 'Request a cross-check', cwd: f.cwd, background: true }, f);
  const waiting = await waitForRun(asking.run_id, { ...f, timeoutSec: 5 });
  assert.equal(waiting.state, 'waiting_gemini', waiting.error);
  const replies = join(runDirectory(asking.run_id, f.sessionsDir), 'tool_replies');
  mkdirSync(replies);
  writeFileSync(join(replies, 'ask_1.json'), '{}');
  const failed = await until(() => {
    const record = readRun(asking.run_id, f.sessionsDir);
    return record.state === 'failed' && record;
  });
  assert.match(failed.error, /Invalid ask_gemini reply/);
  assert.equal(failed.remote_turn_unconfirmed, undefined);
  assert.ok(packets.some(({ method, params }) => method === 'turn/interrupt' && params.turnId === waiting.turn_id));

  const disconnected = await dispatchRun({ goal: 'Disconnect the turn socket', cwd: f.cwd, background: true }, f);
  const outcome = await waitForRun(disconnected.run_id, { ...f, timeoutSec: 5 });
  assert.equal(outcome.state, 'failed', outcome.error);
  assert.equal(outcome.remote_turn_unconfirmed, true);
  assert.match(outcome.error, /Remote Codex turn may still be running/);
  assert.ok(packets.some(({ method }) => method === 'thread/read'));
  assert.ok(!packets.some(({ method, params }) => method === 'turn/interrupt' && params.turnId === outcome.turn_id));
});

test('expired connecting runs and persisted terminal events reconcile without a remote hold', () => {
  const expired = { source: 'magpie_exec', state: 'running', deadline: 1 };
  for (const [pid, message] of [[2_147_483_000, /exited before connecting/], [process.pid, /timed out connecting/]]) {
    const run = reconcileRunState({ ...expired, pid, progress: { phase: 'connecting' } });
    assert.equal(run.state, 'failed');
    assert.match(run.error, message);
    assert.equal(run.remote_turn_unconfirmed, undefined);
  }
  assert.equal(reconcileRunState({ ...expired, thread_id: 'bound', progress: { phase: 'connecting' } }).state, 'outcome_unknown');
  for (const phase of ['interrupted', 'failed', 'completed']) {
    const run = reconcileRunState({ ...expired, turn_id: 'turn', progress: { phase }, remote_turn_unconfirmed: true });
    assert.equal(run.state, phase === 'completed' ? 'finished' : phase);
    assert.equal(run.remote_turn_unconfirmed, undefined);
    if (phase === 'completed') assert.equal(run.result.turn_id, 'turn');
  }
});

test('repeated reads of a dead worker release the session after the unconfirmed hold', async (t) => {
  const f = fixture(t);
  const id = 'b'.repeat(32);
  const old = new Date(Date.now() - 40 * 60_000);
  state(f, id, { source: 'magpie_exec', pid: 2_147_483_000, deadline: old.getTime() / 1000,
    progress: { phase: 'running', turn_id: 'turn' } });
  for (let i = 0; i < 2; i++) {
    const run = readRun(id, f.sessionsDir);
    assert.equal(run.unconfirmed_at, old.toISOString());
    assert.equal(run.remote_turn_unconfirmed, true);
    assert.equal(isRecentUnconfirmedRun(run, old.getTime() + 29 * 60_000), true);
    assert.equal(isRecentUnconfirmedRun(run), false);
    assert.deepEqual(listActiveRoster(f), []);
    await sleep(5);
  }
  assert.equal(JSON.parse(readFileSync(join(runDirectory(id, f.sessionsDir), 'state.json'))).state, 'outcome_unknown');
});

test('dispatch starts the packet worker, records its PID and removes transient files', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  const preload = join(f.root, 'capture-packet.mjs');
  const capture = join(f.root, 'capture.json');
  writeFileSync(preload, [
    "import { readFileSync, writeFileSync, statSync } from 'node:fs';",
    "const [flag, path] = process.argv.slice(2);",
    "const { record } = JSON.parse(readFileSync(path));",
    "writeFileSync(process.env.CAPTURE, JSON.stringify({ flag, path, run: record.run_id, pid: process.pid, mode: statSync(path).mode }));",
  ].join('\n'));
  const env = { ...f.env, GEMPT_DISABLE_CIM: '', CAPTURE: capture, NODE_OPTIONS: '--import=' + pathToFileURL(preload).href };
  const run = await dispatchRun({ cwd: f.cwd, goal: 'Finish', background: true }, { ...f, env });
  const file = join(runDirectory(run.run_id, f.sessionsDir), 'dispatch.json');
  const done = await waitForRun(run.run_id, { ...f, timeoutSec: 8 });
  assert.equal(done.state, 'finished', done.error);
  const actual = JSON.parse(readFileSync(capture));
  assert.deepEqual([actual.flag, actual.path, actual.run, actual.pid], ['--packet', file, run.run_id, run.pid]);
  assert.equal(readRun(run.run_id, f.sessionsDir).pid, actual.pid);
  if (process.platform !== 'win32') assert.equal(actual.mode & 0o777, 0o600);
  assert.ok(!existsSync(file));
  await until(() => !existsSync(join(dirname(file), 'worker.stderr.log')));
});

test('worker exit rereads state past a cached stat before reconciling', async (t) => {
  const f = fixture(t), id = '1'.repeat(32);
  const child = spawn(process.execPath, ['-e', "process.stdin.resume()"], { windowsHide: true });
  const exit = once(child, 'exit');
  state(f, id, { source: 'magpie_exec', pid: child.pid });
  const path = join(runDirectory(id, f.sessionsDir), 'state.json');
  const stat = statSync(path);
  assert.equal(readRun(id, f.sessionsDir, { stat }).state, 'running');
  state(f, id, { state: 'finished', result: { text: 'atomic rename' } });
  child.stdin.end();
  await exit;
  assert.equal(readRun(id, f.sessionsDir, { stat }).result.text, 'atomic rename');
  assert.equal(JSON.parse(readFileSync(path)).state, 'finished');
});

test('dead Codex run recovers only its terminal journal event, text and usage', (t) => {
  const f = fixture(t), id = '3'.repeat(32);
  state(f, id, { source: 'magpie_exec', pid: 2147483000, turn_id: 'tu', codex_thread_id: 'th' });
  const directory = runDirectory(id, f.sessionsDir);
  const events = [
    { method: 'item/completed', params: { item: { id: 'answer', type: 'agentMessage', text: 'final answer' } } },
    { method: 'thread/tokenUsage/updated', runUsage: { input_tokens: 17 }, params: {} },
    { method: 'turn/completed', params: { turn: { id: 'tu', status: 'completed' } } },
    { method: 'turn/completed', params: { turnId: 'other', turn: { id: 'other', status: 'interrupted' } } },
  ];
  writeFileSync(join(directory, 'events.jsonl'), events.map((e) => JSON.stringify({
    ...e, params: { threadId: 'th', turnId: 'tu', ...e.params },
  })).join('\n') + '\n{"partial"');
  const done = readRun(id, f.sessionsDir);
  assert.equal(done.state, 'finished');
  assert.equal(done.result.text, 'final answer');
  assert.deepEqual(done.result.usage, { input_tokens: 17 });
  assert.equal(JSON.parse(readFileSync(join(directory, 'state.json'))).remote_turn_unconfirmed, undefined);
});

test('worker rejoins an in-progress turn after disconnect, receives completion and never starts it twice', async (t) => {
  const f = fixture(t);
  const packets = await appFixture(t, f, { completeOnRejoin: true,
    readThread: (id) => ({ id, turns: [{ id: 'turn-1', status: 'inProgress' }] }) });
  const run = await dispatchRun({ goal: 'Disconnect the turn socket', cwd: f.cwd, background: true }, f);
  const done = await waitForRun(run.run_id, { ...f, timeoutSec: 5 });
  assert.equal(done.state, 'finished', done.error);
  assert.match(done.result.text, /after reconnect/);
  assert.equal(packets.filter((p) => p.method === 'turn/start').length, 1);
  assert.ok(packets.some((p) => p.method === 'thread/resume'));
  assert.ok(!packets.some((p) => p.method === 'turn/interrupt'));
});

test('thread/read heals an orphan and interrupt recovers a completed turn on no-active RPC', async (t) => {
  const f = fixture(t), id = '4'.repeat(32);
  const controls = { interruptError: 'no active turn to interrupt', readThread: (id) => ({
    id, turns: [{ id: 'tu', status: 'completed', items: [{ type: 'agentMessage', text: 'remote result' }] }],
  }) };
  const packets = await appFixture(t, f, controls);
  const seed = () => state(f, id, { state: 'outcome_unknown', remote_turn_unconfirmed: true,
    codex_thread_id: 'th', turn_id: 'tu', progress: { token_usage: { input_tokens: 11 } } });
  seed();
  await ensureSwarm({ swarmId: 'orphan', cwd: f.cwd, sessionsDir: f.sessionsDir });
  await upsertWorker('orphan', { name: 'builder', state: 'working', parked_on: 'ask', run_id: id }, f);
  const orphan = readRun(id, f.sessionsDir);
  state(f, id, { ...orphan, swarm_id: 'orphan', swarm_agent: 'builder' });
  const run = await waitForRun(id, { ...f, timeoutSec: 0 });
  assert.equal(run.state, 'finished');
  assert.equal(run.result.text, 'remote result');
  assert.deepEqual(run.result.usage, { input_tokens: 11 });
  assert.equal(run.remote_turn_unconfirmed, undefined);
  const worker = await getWorker('orphan', 'builder', f);
  assert.equal(worker.state, 'waiting');
  assert.equal(worker.parked_on, null);
  const posts = await readBoardMessages('orphan', f);
  assert.ok(posts.some((msg) => msg.meta?.run_id === id && msg.text.includes('remote result')));
  seed();
  await interruptRun(id, f);
  assert.equal(readRun(id, f.sessionsDir).result.text, 'remote result');
  assert.equal(packets.filter((p) => p.method === 'thread/read').length, 2);
});

test('worker logs RPC start rejection without marking the turn unconfirmed and removes empty stderr on success', async (t) => {
  const f = fixture(t);
  const controls = { rejectStart: true };
  const packets = await appFixture(t, f, controls);
  const dispatch = () => dispatchRun({ goal: 'Check worker', cwd: f.cwd, background: true }, f);
  const failed = await waitForRun((await dispatch()).run_id, { ...f, timeoutSec: 5 });
  assert.equal(failed.state, 'failed');
  assert.match(failed.error, /Rejected turn\/start/);
  assert.equal(failed.remote_turn_unconfirmed, undefined);
  assert.equal(packets.some((p) => p.method === 'turn/interrupt'), false);
  assert.match(readFileSync(join(runDirectory(failed.run_id, f.sessionsDir), 'worker.stderr.log'), 'utf8'), /Rejected turn\/start/);

  controls.rejectStart = false;
  const success = await waitForRun((await dispatch()).run_id, { ...f, timeoutSec: 5 });
  assert.equal(success.state, 'finished', success.error);
  await until(() => !existsSync(join(runDirectory(success.run_id, f.sessionsDir), 'worker.stderr.log')));
});

test('worker retains final state read diagnostics and restores a failed state from its dispatch record', async (t) => {
  const f = fixture(t);
  const id = 'c'.repeat(32);
  const directory = runDirectory(id, f.sessionsDir);
  const file = join(directory, 'state.json');
  await appFixture(t, f, { rejectStart: true, beforeReject: () => writeFileSync(file, 'corrupt') });
  await dispatchRun({ run_id: id, goal: 'Check finalization', cwd: f.cwd, background: true }, f);
  const failed = await until(() => {
    const raw = readFileSync(file, 'utf8');
    if (raw === 'corrupt') return false;
    const run = JSON.parse(raw);
    return run.state === 'failed' && run;
  });
  assert.match(failed.error, /JSON/);
  assert.equal(failed.progress.phase, 'failed');
  assert.ok(failed.finished_at);
  assert.match(readFileSync(join(directory, 'worker.stderr.log'), 'utf8'), /readRun/);
});

test('worker retains a final save error and persists failure on the next write', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  const preload = join(f.root, 'fail-finished-write.mjs');
  writeFileSync(preload, `
    import fs from 'node:fs';
    import { syncBuiltinESMExports } from 'node:module';
    import { basename } from 'node:path';
    const rename = fs.renameSync;
    fs.renameSync = (src, dest) => {
      if (basename(dest) === 'state.json' && JSON.parse(fs.readFileSync(src)).state === 'finished') {
        throw new Error('Cannot persist final result');
      }
      return rename(src, dest);
    };
    syncBuiltinESMExports();
  `);
  const env = { ...f.env, NODE_OPTIONS: `--import=${pathToFileURL(preload).href}` };
  const queued = await dispatchRun({ goal: 'Complete a turn', cwd: f.cwd, background: true }, { ...f, env });
  const failed = await waitForRun(queued.run_id, { ...f, timeoutSec: 5 });
  assert.equal(failed.state, 'failed');
  assert.equal(failed.error, 'Cannot persist final result');
  assert.equal(failed.remote_turn_unconfirmed, undefined);
  assert.match(readFileSync(join(runDirectory(queued.run_id, f.sessionsDir), 'worker.stderr.log'), 'utf8'), /saveRun/);
});

test('worker records uncaught exceptions and unhandled rejections during a live turn', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  const preload = join(f.root, 'worker-fault.mjs');
  writeFileSync(preload, `
    import { existsSync } from 'node:fs';
    const timer = setInterval(() => {
      if (!existsSync(process.env.FAULT_TRIGGER)) return;
      clearInterval(timer);
      const err = new Error('Worker fault during live turn');
      if (process.env.FAULT_KIND === 'uncaughtException') throw err;
      Promise.reject(err);
    }, 10);
    timer.unref();
  `);
  for (const kind of ['uncaughtException', 'unhandledRejection']) {
    const trigger = join(f.root, kind);
    const env = { ...f.env, NODE_OPTIONS: `--import=${pathToFileURL(preload).href}`, FAULT_TRIGGER: trigger, FAULT_KIND: kind };
    const { run_id } = await dispatchRun({ goal: 'Request a cross-check', cwd: f.cwd, background: true }, { ...f, env });
    assert.equal((await waitForRun(run_id, { ...f, timeoutSec: 5 })).state, 'waiting_gemini');
    writeFileSync(trigger, '');
    const failed = await until(() => {
      const run = readRun(run_id, f.sessionsDir);
      return run.state === 'failed' && run;
    });
    assert.equal(failed.error, 'Worker fault during live turn');
    assert.equal(failed.pending_tool_call, null);
    assert.equal(failed.remote_turn_unconfirmed, true);
    assert.match(readFileSync(join(runDirectory(run_id, f.sessionsDir), 'worker.stderr.log'), 'utf8'), new RegExp(kind));
  }
});

test('automatic interrupt accepts terminal RPC errors and reconnect reads a rotated endpoint', async (t) => {
  const f = fixture(t);
  const controls = { interruptError: 'Turn already completed' };
  const packets = await appFixture(t, f, controls);
  const asking = await dispatchRun({ goal: 'Request a cross-check', cwd: f.cwd, background: true }, f);
  const waiting = await waitForRun(asking.run_id, { ...f, timeoutSec: 5 });
  assert.equal(waiting.state, 'waiting_gemini', waiting.error);
  const replies = join(runDirectory(asking.run_id, f.sessionsDir), 'tool_replies');
  mkdirSync(replies);
  writeFileSync(join(replies, 'ask_1.json'), '{}');
  const failed = await until(() => {
    const run = readRun(asking.run_id, f.sessionsDir);
    return run.state === 'failed' && run;
  });
  assert.match(failed.error, /Invalid ask_gemini reply/);
  assert.equal(failed.remote_turn_unconfirmed, undefined);
  assert.ok(packets.some((p) => p.method === 'turn/interrupt' && p.params.turnId === waiting.turn_id));

  const replacement = fixture(t);
  const recovered = await appFixture(t, replacement, { token: 'rotated-token',
    readThread: (id) => ({ id, status: { type: 'idle' }, turns: [] }) });
  const published = f.env.GEMPT_APP_SERVER_STATE;
  controls.beforeDisconnect = () => copyFileSync(replacement.env.GEMPT_APP_SERVER_STATE, published);
  const dead = await dispatchRun({ goal: 'Disconnect the turn socket', cwd: f.cwd, background: true }, f);
  const outcome = await waitForRun(dead.run_id, { ...f, timeoutSec: 5 });
  assert.equal(outcome.state, 'failed', outcome.error);
  assert.equal(outcome.remote_turn_unconfirmed, undefined);
  assert.ok(recovered.some((p) => p.method === 'thread/read' && p.params.threadId === outcome.codex_thread_id));
});

test('expired queued claims release occupancy, but old active sessions still protect their worktree', async (t) => {
  const f = fixture(t);
  initRepo(f.cwd);
  const sessionId = 'occupied';
  const worktree = ensureShareWorktree(f.cwd, sessionId, f.env).worktree_root;
  const id = 'f'.repeat(32);
  const file = join(runDirectory(id, f.sessionsDir), 'state.json');
  const old = new Date(Date.now() - 120_000);
  state(f, id, { session_id: sessionId, worktree_path: worktree, state: 'waiting_gemini' });
  utimesSync(file, old, old);
  for (let i = 1; i <= 101; i++) state(f, i.toString(16).padStart(32, '0'), {});
  await assert.rejects(dispatchRun({ goal: 'Resume', cwd: f.cwd, session_id: sessionId, background: true }, f),
    (error) => error.status === 409 && error.message.includes(id));
  assert.deepEqual(pruneShareWorktrees(f.cwd, { env: f.env, maxAgeMs: 0 }), { removed: [], kept: [worktree] });

  state(f, id, { session_id: sessionId, worktree_path: worktree, state: 'queued', created_at: old.toISOString(),
    deadline: Date.now() / 1000 + 4500, source: 'magpie_exec' });
  assert.match(readRun(id, f.sessionsDir).error, /worker startup timed out/);
  const worker = fileURLToPath(new URL('../magpie_plugin/lib/exec-worker.mjs', import.meta.url));
  const queued = readFileSync(file, 'utf8');
  const late = spawnSync(process.execPath, [worker], { env: f.env, encoding: 'utf8', windowsHide: true,
    input: JSON.stringify({ file, record: JSON.parse(queued), host: { sessionsDir: f.sessionsDir } }) });
  assert.equal(late.status, 0, late.stderr);
  assert.equal(readFileSync(file, 'utf8'), queued);
  assert.equal(listActiveRoster({ ...f, sessionId }).length, 0);
  assert.deepEqual(pruneShareWorktrees(f.cwd, { env: f.env, maxAgeMs: 0 }), { removed: [worktree], kept: [] });
});

test('wait batch returns the first ready run or waits for all and keeps waiting_gemini ready', async (t) => {
  const f = fixture(t);
  const first = 'a'.repeat(32);
  const second = 'b'.repeat(32);
  state(f, first, { state: 'waiting_gemini', host_conversation_id: 'host' });
  state(f, second, { state: 'running', host_conversation_id: 'another' });
  assert.equal(listActiveRoster({ ...f, hostConversationId: 'host' })[0].run_id, first);
  assert.deepEqual((await waitForRuns([first, second], f)).ready, [first]);
  assert.deepEqual((await waitForRuns([first, second], { ...f, waitAll: true, timeoutSec: 0 })).active, [second]);
  const completion = sleep(40).then(() => state(f, second, { state: 'finished' }));
  const batch = await waitForRuns(`${first},${second}`, { ...f, waitAll: true, timeoutSec: 2, pollInterval: 0.01 });
  await completion;
  assert.deepEqual(batch.ready, [first, second]);
  assert.deepEqual(batch.active, []);
  assert.equal(batch.runs[second].state, 'finished');
  assert.match(buildWaitCommand([first, second], { waitAll: true }), new RegExp(`--all ${first},${second}$`));
  const waitScript = fileURLToPath(new URL('../core/wait.mjs', import.meta.url));
  for (const flags of [[], ['--all']]) {
    const result = spawnSync(process.execPath, [waitScript, ...flags, `${first},${second}`], { env: f.env, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(JSON.parse(result.stdout).ready, batch.ready);
  }
  state(f, second, { state: 'failed', error: 'fixture error' });
  const failed = spawnSync(process.execPath, [waitScript, '--all', `${first},${second}`], { env: f.env, encoding: 'utf8' });
  assert.equal(failed.status, 1, failed.stderr);
  assert.equal(JSON.parse(failed.stdout).runs[first].state, 'waiting_gemini');
});

test('Live run list binds to the active conversation and workspace', async (t) => {
  const f = fixture(t);
  const first = '1'.repeat(32);
  await updateHostContext('current', [f.cwd], f);
  state(f, first, { host_conversation_id: 'current' });
  state(f, '2'.repeat(32), { host_conversation_id: 'another' });
  state(f, '3'.repeat(32), { host_conversation_id: 'current', cwd: f.root });
  const opts = { ...f, root: f.cwd };
  const scoped = await runLiveOperation('runs', {}, opts);
  assert.equal(scoped.bound_conversation_id, 'current');
  assert.equal(scoped.bound_workspace_name, 'repo');
  assert.equal(scoped.session_count, 1);
  assert.deepEqual(scoped.runs.map(({ run_id }) => run_id), [first]);
  assert.equal((await runLiveOperation('runs', { scope: 'all' }, opts)).runs.length, 2);
  assert.equal((await runLiveOperation('runs', { host_conversation_id: 'another' }, opts)).session_count, 1);
  await assert.rejects(runLiveOperation('stream', { run_id: '3'.repeat(32) }, opts), /outside host workspace/);
});

test('Live run list defaults to an empty unbound session and can list all workspace runs', async (t) => {
  const f = fixture(t);
  const first = '1'.repeat(32);
  const second = '2'.repeat(32);
  state(f, first, { host_conversation_id: 'previous' });
  state(f, second, { host_conversation_id: 'another', state: 'finished' });
  const opts = { ...f, root: f.cwd };
  const scoped = await runLiveOperation('runs', {}, opts);
  assert.equal(scoped.scope, 'session');
  assert.equal(scoped.unbound_session, true);
  assert.deepEqual(scoped.runs, []);
  assert.equal(scoped.app_server.version, '0.10.1');

  const all = await runLiveOperation('runs', { scope: 'all' }, opts);
  assert.equal(all.scope, 'all');
  assert.equal(all.unbound_session, false);
  assert.deepEqual(all.runs.map(({ run_id }) => run_id).sort(), [first, second]);
  assert.equal(all.app_server.version, '0.10.1');
});

test('Live binds normalized workspaces and never selects another run for a filtered explicit ID', async (t) => {
  const f = fixture(t);
  const other = join(f.root, 'other');
  mkdirSync(other);
  await updateHostContext('first', [f.cwd + '/.'], { ...f, geminiFamily: 'pro' });
  await updateHostContext('elsewhere', [other], { ...f, geminiFamily: 'flash' });
  const nested = join(f.cwd, 'nested');
  mkdirSync(nested);
  const workspaceContext = readHostContext(f.configPath, nested);
  assert.equal(workspaceContext.conversation_id, 'first');
  assert.equal(workspaceContext.gemini_family, 'pro');
  assert.equal(readHostContext(f.configPath).conversation_id, 'elsewhere');
  await updateHostContext('child', [f.cwd], { ...f, geminiFamily: 'flash' });
  await updateHostContext('child', [f.cwd], { ...f, parentId: 'first', geminiFamily: 'flash' });
  assert.equal(readHostContext(f.configPath, nested).gemini_family, 'pro');
  assert.equal(readHostContext(f.configPath, other).conversation_id, 'elsewhere');
  assert.deepEqual(readHostContext(f.configPath, f.root), {});
  await updateHostContext('latest', [f.cwd], f);
  await updateHostContext('first', undefined, f);
  assert.equal(readHostContext(f.configPath, nested).conversation_id, 'first');
  assert.equal(readHostContext(f.configPath, nested).gemini_family, 'pro');

  const current = '1'.repeat(32);
  const previous = '2'.repeat(32);
  state(f, current, { host_conversation_id: 'first' });
  state(f, previous, { host_conversation_id: 'previous' });
  for (let i = 3; i <= 23; i++) state(f, i.toString(16).padStart(32, '0'), { host_conversation_id: 'previous' });
  const opts = { ...f, root: f.cwd };
  const excluded = await runLiveOperation('open', { run_id: previous }, opts);
  assert.equal(excluded.active_conversation_id, 'first');
  assert.equal(excluded.requested_run, false);
  assert.equal(excluded.run_id, null);
  assert.equal(excluded.stream, null);
  assert.deepEqual(excluded.runs.map(({ run_id }) => run_id), [current]);
  const ctlExcluded = await handleTool('gpt_ctl', { run_id: previous }, f.env, opts);
  assert.equal(ctlExcluded.structuredContent, undefined);
  assert.equal(JSON.parse(ctlExcluded.content[0].text).id, previous);
  const all = await runLiveOperation('open', { scope: 'all', run_id: previous }, opts);
  assert.equal(all.run_id, previous);
  assert.equal(all.requested_run, true);
  assert.equal(all.runs.length, 23);
  const page = await runLiveOperation('runs', { scope: 'all', limit: 3 }, opts);
  assert.equal(page.next_offset, 3);
  const rest = await runLiveOperation('runs', { scope: 'all', offset: page.next_offset }, opts);
  assert.deepEqual([...page.runs, ...rest.runs].map(({ run_id }) => run_id), all.runs.map(({ run_id }) => run_id));
  const bound = await runLiveOperation('open', { host_conversation_id: 'previous' }, opts);
  assert.equal(bound.bound_conversation_id, 'previous');
  assert.equal(bound.active_conversation_id, 'first');
  const ctlBound = await handleTool('gpt_ctl', { host_conversation_id: 'previous' }, f.env, opts);
  assert.ok(JSON.parse(ctlBound.content[0].text).active.length >= 1);

  for (const [name, fields] of Object.entries({ gpt_run: ['title', 'tier', 'background', 'host_conversation_id'],
    gpt_ask: ['host_conversation_id'], gpt_ctl: ['host_conversation_id'] })) {
    const { inputSchema } = TOOLS.find((tool) => tool.name === name);
    for (const field of fields) assert.ok(Object.hasOwn(inputSchema.properties, field), `${name}.${field}`);
  }
});


test('PreToolUse binds exact cross-repo directories briefly while preserving interleaved workspace ownership', async (t) => {
  const f = fixture(t);
  const env = { ...f.env, GEMPT_WORKSPACE_ROOT: '' };
  const workspaceB = join(f.root, 'workspace-b');
  const crossA = join(f.root, 'cross-a');
  const crossB = join(f.root, 'cross-b');
  const nested = join(crossA, 'nested');
  const file = join(f.root, 'plain-file');
  for (const dir of [workspaceB, crossA, crossB, nested]) mkdirSync(dir);
  writeFileSync(file, '');
  await updateHostContext('conversation-a', [f.cwd], { ...f, geminiFamily: 'neon' });
  await updateHostContext('conversation-b', [workspaceB], { ...f, geminiFamily: 'carbon' });
  const hook = (payload) => jsonProcess(process.execPath, [hookPath], { env, input: JSON.stringify(payload) });
  const invoke = (conversation_id, tool_name, cwd) => hook({
    hook_event_name: 'PreToolUse', conversation_id, tool_name, tool_input: { cwd },
  });
  for (const [tool, conversation, cwd] of [
    ['gpt_run', 'conversation-a', crossA], ['gpt_swarm', 'conversation-b', crossB],
    ['gpt_ask', 'conversation-a', crossA], ['gpt_ctl', 'conversation-b', crossB],
    ['gpt_board', 'conversation-a', crossA],
  ]) {
    assert.deepEqual(invoke(conversation, tool, cwd), { decision: 'allow' });
    assert.equal(readHostContext(f.configPath, cwd).conversation_id, conversation);
  }
  assert.equal(readHostContext(f.configPath, crossA).gemini_family, 'neon');
  assert.equal(readHostContext(f.configPath, crossB).gemini_family, 'carbon');
  assert.deepEqual(readHostContext(f.configPath, nested), {});
  assert.deepEqual(invoke('conversation-a', 'gpt_run', workspaceB), { decision: 'allow' });
  assert.equal(readHostContext(f.configPath, workspaceB).conversation_id, 'conversation-b');
  assert.equal(readHostContext(f.configPath, f.cwd).conversation_id, 'conversation-a');
  assert.equal(readHostContext(f.configPath).conversation_id, 'conversation-b');
  assert.deepEqual(invoke('conversation-a', 'gpt_run', file), { decision: 'allow' });
  assert.deepEqual(readHostContext(f.configPath, file), {});
  hook({ hookEventName: 'PreToolUse', conversationId: 'conversation-a',
    toolCall: { name: 'call_mcp_tool', args: { ServerName: 'other', ToolName: 'gpt_run', Arguments: { cwd: nested } } } });
  assert.deepEqual(readHostContext(f.configPath, nested), {});
  const path = contextPath(f.configPath);
  const data = JSON.parse(readFileSync(path, 'utf8'));
  const binding = Object.values(data.by_tool_cwd).find((entry) => entry.conversation_id === 'conversation-b');
  binding.recorded_at_ms = Date.now() - 60_000;
  writeFileSync(path, JSON.stringify(data));
  assert.deepEqual(readHostContext(f.configPath, crossB), {});
  assert.equal(readHostContext(f.configPath, crossA).conversation_id, 'conversation-a');
  assert.equal(readHostContext(f.configPath, workspaceB).conversation_id, 'conversation-b');
});

test('Live run index sees new directories, worker updates and removed state files', async (t) => {
  const f = fixture(t);
  const first = '1'.repeat(32);
  const second = '2'.repeat(32);
  const runs = async () => (await runLiveOperation('runs', { scope: 'all' }, f)).runs;
  assert.deepEqual(await runs(), []);
  state(f, first, { state: 'finished', session_id: 'session-a' });
  assert.deepEqual((await runs()).map((run) => run.state), ['finished']);
  state(f, first, { state: 'running', session_id: 'session-a' });
  assert.deepEqual((await runs()).map((run) => run.state), ['running']);
  state(f, second, { state: 'finished', session_id: 'session-b' });
  assert.deepEqual(new Set((await runs()).map((run) => run.run_id)), new Set([first, second]));
  const linked = await runLiveOperation('runs', { scope: 'all', session_id: 'session-a' }, f);
  assert.deepEqual(linked.runs.map((run) => run.run_id), [first]);
  rmSync(join(runDirectory(first, f.sessionsDir), 'state.json'));
  assert.deepEqual((await runs()).map((run) => run.run_id), [second]);
});

test('Live journal filters other turns, bounds events, and pages stable Unicode text and goals', async (t) => {
  const f = fixture(t);
  const id = '4'.repeat(32);
  const full = '🧪\n'.repeat(20_000);
  const goal = '目标 🧪'.repeat(3000);
  state(f, id, { codex_thread_id: 'ours', turn_id: '1', goal });
  const journal = join(runDirectory(id, f.sessionsDir), 'events.jsonl');
  const event = (method, params) => JSON.stringify({ method, params: { threadId: 'ours', turnId: '1', ...params } }) + '\n';
  const unrelated = event('item/agentMessage/delta', { threadId: 'other', itemId: 'no', delta: 'unrelated' });
  const command = event('item/completed', { item: { id: 'cmd', type: 'commandExecution', command: 'test', exitCode: 0 } });
  const message = event('item/agentMessage/delta', { itemId: 'answer', delta: full });
  const incomplete = event('item/agentMessage/delta', { itemId: 'answer', delta: ' END' }).trimEnd();
  const parse = JSON.parse;
  let parsedDiffs = 0;
  t.mock.method(JSON, 'parse', (value, ...options) => {
    if (typeof value === 'string' && value.startsWith('{"method":"turn/diff/updated"')) parsedDiffs++;
    return parse(value, ...options);
  });
  const diff = event('turn/diff/updated', { diff: 'diff only 🧪'.repeat(200_000) });
  writeFileSync(journal, unrelated + command + diff + message + incomplete);
  const call = async (args) => runLiveOperation('stream', { run_id: id, ...args }, f);
  const opened = await runLiveOperation('open', { scope: 'all' }, f);
  assert.equal(opened.runs[0].run_id, id);
  assert.equal(opened.stream, null);
  const ctlOpened = await handleTool('gpt_ctl', { scope: 'all' }, f.env, f);
  assert.equal(JSON.parse(ctlOpened.content[0].text).active[0].id, id);
  const first = await call({ max_events: 1 });
  assert.equal((await call({ max_events: 1 })).text_revision, first.text_revision);
  assert.equal(first.timeline.length, 1);
  assert.equal(first.timeline[0].type, 'agent_message');
  assert.equal(first.commands[0].exit_code, 0);
  assert.ok(first.next_text_offset > 0 && first.text_revision > 0);
  assert.ok(Buffer.byteLength(JSON.stringify(first.full_text)) <= 96 * 1024);
  appendFileSync(journal, '\n');
  const updated = await call({});
  assert.notEqual(updated.text_revision, first.text_revision);
  const page = await call({ text_offset: first.next_text_offset, text_revision: first.text_revision });
  assert.equal(first.full_text + page.full_text, full);
  const currentPage = await call({ text_offset: updated.next_text_offset, text_revision: updated.text_revision });
  assert.equal(updated.full_text + currentPage.full_text, full + ' END');
  let reconstructedGoal = first.run.goal;
  let goalOffset = first.next_goal_offset;
  while (goalOffset !== null) {
    const page = await call({ goal_offset: goalOffset });
    reconstructedGoal += page.goal;
    goalOffset = page.next_goal_offset;
  }
  assert.equal(reconstructedGoal, goal);
  assert.deepEqual(await call({ text_offset: 20, text_revision: first.text_revision + 100 }), { stale_text_revision: true });
  appendFileSync(journal, diff.slice(0, -1));
  assert.equal((await call({})).text_revision, updated.text_revision);
  appendFileSync(journal, '\n');
  assert.equal((await call({})).text_revision, updated.text_revision);
  writeFileSync(journal, event('item/agentMessage/delta', { itemId: 'answer', delta: 'rotated' }));
  assert.equal(parsedDiffs, 0, 'large diff notifications are skipped before JSON parsing');
  assert.equal((await call({})).full_text, 'rotated');
});

test('parseToolArguments accepts objects and JSON object strings and rejects malformed or non-object values', () => {
  const args = { action: 'read', seqs: [1, 2] };
  assert.equal(parseToolArguments(args), args);
  assert.deepEqual(parseToolArguments(` ${JSON.stringify(args)} `), args);
  for (const raw of [undefined, null, '', ' \t\n']) {
    assert.deepEqual(parseToolArguments(raw), {});
  }
  for (const raw of ['{', '[]', 'null', 'false', '0', '"text"', [], false, 0, new Date()]) {
    assert.throws(() => parseToolArguments(raw), { name: 'Error', message: 'Tool arguments must be a JSON object.' });
  }
});

test('standalone NDJSON MCP serves the inline launcher and preserves cross-directory tool receipts and ownership', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  await updateHostContext('host', [f.cwd], f);
  state(f, 'd'.repeat(32), { goal: '🚦'.repeat(120), host_conversation_id: 'host' });
  const linkedCore = join(f.root, 'linked core');
  symlinkSync(fileURLToPath(new URL('../core/', import.meta.url)), linkedCore, process.platform === 'win32' ? 'junction' : 'dir');
  const file = join(linkedCore, 'mcp-server.mjs');
  const env = { ...f.env, GEMPT_WORKSPACE_ROOT: '' };
  const cross = join(f.root, 'cross-repo');
  mkdirSync(cross);
  assert.deepEqual(jsonProcess(process.execPath, [hookPath], { env, input: JSON.stringify({
    hookEventName: 'PreToolUse', conversationId: 'host', workspacePaths: [f.cwd],
    toolCall: { name: 'call_mcp_tool', args: { ServerName: 'gempt_gempt', ToolName: 'gpt_run', Arguments: { cwd: cross } } },
  }) }), { decision: 'allow' });
  const child = spawn(process.execPath, [file], { env, stdio: ['pipe', 'pipe', 'pipe'] });
  t.after(() => child.kill());
  const exited = once(child, 'exit');
  const responses = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  let id = 0;
  const send = (message) => child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', ...message })}\n`);
  async function request(method, params) {
    send({ method, params, id: ++id });
    const line = await responses.next();
    assert.equal(line.done, false);
    const data = JSON.parse(line.value);
    assert.equal(data.id, id);
    return data;
  }
  const started = await request('initialize', { protocolVersion: '2026-test' });
  assert.equal(started.result.protocolVersion, '2026-test');
  assert.equal(started.result.serverInfo.name, 'gempt');
  assert.equal(started.result.serverInfo.version, '0.10.1');
  assert.deepEqual(started.result.capabilities, { tools: { listChanged: false }, resources: {} });
  send({ method: 'notifications/initialized' });
  assert.deepEqual((await request('ping')).result, {});
  const { tools } = (await request('tools/list')).result;
  assert.deepEqual(tools.map(({ name }) => name), ['gpt_run', 'gpt_ask', 'gpt_ctl', 'gpt_swarm', 'gpt_board']);
  assert.deepEqual(tools.filter(({ _meta }) => _meta).map(({ name, _meta }) => ({ name, _meta })),
    ['gpt_run', 'gpt_swarm'].map((name) => ({ name, _meta: { ui: { resourceUri: launcherResource.uri } } })));
  assert.ok(tools.every((tool) => !Object.hasOwn(tool, 'meta')));
  assert.deepEqual((await request('resources/list')).result.resources, [{ ...launcherResource, name: 'GemPT launcher' }]);
  assert.deepEqual((await request('resources/read', { uri: launcherResource.uri })).result.contents,
    [{ ...launcherResource, text: readFileSync(launcherUrl, 'utf8') }]);
  assert.equal((await request('resources/read', { uri: 'ui://unknown' })).error.code, -32002);
  assert.equal((await request('resources/read', { uri: 'ui://gempt/live.html' })).error.code, -32002);
  const { result } = await request('tools/call', { name: 'gpt_ctl', arguments: {} });
  const status = JSON.parse(result.content[0].text);
  assert.equal(status.active.length, 1);
  assert.equal(status.active[0].id, 'd'.repeat(32));
  assert.equal(result.structuredContent, undefined);
  const invalid = await request('tools/call', { name: 'gpt_ctl', arguments: '{' });
  assert.deepEqual(invalid.error, { code: -32603, message: 'Tool arguments must be a JSON object.' });
  const oversized = 'e'.repeat(32);
  state(f, oversized, { state: 'failed', host_conversation_id: 'host', title: '🚦'.repeat(40),
    error: '🚧'.repeat(500) });
  const detail = (await request('tools/call', { name: 'gpt_ctl', arguments: JSON.stringify({ run_id: oversized }) })).result;
  assert.equal(JSON.parse(detail.content[0].text).err, '🚧'.repeat(500));
  assert.equal(JSON.parse(detail.content[0].text).id, oversized);
  const asked = await request('tools/call', { name: 'gpt_ask', arguments: { goal: 'Summarize the fixture.', cwd: cross } });
  assert.equal(asked.result.isError, undefined, asked.result.content[0].text);
  assert.equal(JSON.parse(asked.result.content[0].text).text.includes('Consult answer.'), true);
  const panel = { ...f, workspaceRoots: [f.cwd], conversationId: 'host' };
  const consults = await runLiveOperation('runs', { include_consults: true }, panel);
  assert.equal(consults.consult_sessions[0].session_id,
    JSON.parse(asked.result.content[0].text).session_id);
  const startedAt = Date.now();
  const launch = await request('tools/call', { name: 'gpt_run', arguments: {
    goal: 'Inspect the fixture.', title: 'Cross-repo inspection', access: 'inspect', cwd: cross,
  } });
  assert.equal(launch.result.isError, false, launch.result.content[0].text);
  const run = JSON.parse(launch.result.content[0].text);
  assert.match(run.wait_command, new RegExp(`${run.run_id}$`));
  assert.equal(run.text, `Run ${run.run_id} started. Execute \`${run.wait_command}\` via run_command(WaitMsBeforeAsync=500) for zero-poll wakeup.`);
  const { created_at_ms: runTime, ...runSummary } = launch.result.structuredContent;
  assert.ok(Number.isInteger(runTime) && runTime >= startedAt && runTime <= Date.now());
  assert.deepEqual(runSummary, { kind: 'run', action: 'start', title: 'Cross-repo inspection', state: run.state,
    run_id: run.run_id, member_count: 1, sidecar_uri: 'sidecar://gempt/gempt_live/', auto_open_panel: true });
  const complete = await waitForRun(run.run_id, { ...f, timeoutSec: 5 });
  assert.equal(complete.result?.sandbox, 'read-only', complete.error);
  assert.equal(complete.execution_timeout_sec, null);
  assert.equal(complete.host_conversation_id, 'host');
  assert.equal((await runLiveOperation('open', { run_id: run.run_id }, panel)).run_id, run.run_id);
  const otherPanel = { ...panel, conversationId: 'other' };
  assert.ok(!(await runLiveOperation('runs', { host_conversation_id: 'host' }, otherPanel))
    .runs.some((entry) => entry.run_id === run.run_id));
  await assert.rejects(runLiveOperation('stream', { run_id: run.run_id }, otherPanel), /Run outside host workspace/);

  const swarm = (action, args = {}) => request('tools/call', { name: 'gpt_swarm', arguments: { action, cwd: cross, ...args } });
  const titled = await swarm('title', { title: 'Cross-repo workers' });
  assert.equal(titled.result.structuredContent.auto_open_panel, false);
  const added = await swarm('add', { name: 'reader', brief: 'Inspect the fixture.', access: 'inspect', cli: 'codex' });
  assert.notEqual(added.result.isError, true, added.result.content[0].text);
  const addReceipt = JSON.parse(added.result.content[0].text);
  assert.equal(addReceipt.ok, true);
  assert.equal(addReceipt.workers[0].name, 'reader');
  assert.match(addReceipt.wait_command, /--board .* --since \d+$/);
  const swarmId = 'conversation-' + createHash('sha256').update('host').digest('hex');
  assert.equal(addReceipt.swarm_id, swarmId);
  const { created_at_ms: swarmTime, state: swarmState, ...swarmSummary } = added.result.structuredContent;
  assert.ok(Number.isInteger(swarmTime) && swarmTime >= runTime && swarmTime <= Date.now());
  assert.ok(['working', 'idle'].includes(swarmState));
  assert.deepEqual(swarmSummary, { kind: 'swarm', action: 'add', title: 'Cross-repo workers',
    swarm_id: swarmId, member_count: 2, sidecar_uri: 'sidecar://gempt/gempt_live/', auto_open_panel: true });
  const statusResult = (await swarm('status')).result;
  const statusReceipt = JSON.parse(statusResult.content[0].text);
  assert.equal(statusReceipt.swarm_id, swarmId);
  assert.equal(statusReceipt.roster.length, 3);
  assert.ok(statusReceipt.roster.some((member) => member.name === 'coordinator' && member.conversation_id === 'host'));
  assert.equal(statusReceipt.open_board, 'sidecar://gempt/gempt_live/');
  assert.equal(statusResult.structuredContent.auto_open_panel, false);
  assert.equal(statusResult.structuredContent.member_count, 2);
  assert.equal((await waitForRun(addReceipt.workers[0].run_id, { ...f, timeoutSec: 5 })).host_conversation_id, 'host');
  child.stdin.end();
  assert.deepEqual(await exited, [0, null]);
});

test('waiting_gemini pauses execution timeout and deadline, and gpt_run validates timeout_sec', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  const gateway = JSON.parse(readFileSync(f.configPath, 'utf8')).magpie.base_url;
  const runId = 'c'.repeat(32);
  const runFile = join(runDirectory(runId, f.sessionsDir), 'state.json');
  const initialDeadline = Date.now() / 1000 + 30;
  saveRun(runFile, {
    source: 'magpie_exec', run_id: runId, session_id: 'exec-pause-test', cwd: f.cwd,
    state: 'running', pid: process.pid, deadline: initialDeadline,
  });

  const runPromise = runAppServer({
    configPath: f.configPath, env: f.env, runFile, sessionId: 'exec-pause-test',
    onIdentity() {}, prompt: 'Request a cross-check', cwd: f.cwd,
    model: 'codex/gpt-6.2-sol', effort: 'medium', sandbox: 'workspace-write',
    approvalPolicy: 'never', gatewayUrl: gateway, timeoutSec: 0.45,
  });

  const waiting = await until(() => {
    const rec = readRun(runId, f.sessionsDir);
    return rec.state === 'waiting_gemini' && rec;
  });
  assert.equal(waiting.pending_tool_call.call_id, 'ask_1');
  assert.ok(waiting.waiting_since);

  // Past record.deadline must not expire while in waiting_gemini with a live worker PID.
  const waitingReconciled = reconcileRunState({
    source: 'magpie_exec', run_id: runId, state: 'waiting_gemini', pid: process.pid,
    deadline: Date.now() / 1000 - 3600, turn_id: 'turn-1',
  });
  assert.equal(waitingReconciled.state, 'waiting_gemini');
  // Dead worker PID in waiting_gemini still reconciles to outcome_unknown + remote_turn_unconfirmed.
  const exited = spawnSync(process.execPath, ['-e', 'process.exit(0)']);
  const deadWaiting = reconcileRunState({
    source: 'magpie_exec', run_id: runId, state: 'waiting_gemini', pid: exited.pid,
    deadline: Date.now() / 1000 + 3600, turn_id: 'turn-1',
  });
  assert.equal(deadWaiting.state, 'outcome_unknown');
  assert.equal(deadWaiting.remote_turn_unconfirmed, true);

  // Wait longer than timeoutSec (0.45s) before replying; paused timer must not fire.
  await sleep(650);
  assert.equal(readRun(runId, f.sessionsDir).state, 'waiting_gemini');
  assert.equal(replyToolCall(runId, 'ask_1', 'Approved.', f).ok, true);

  const result = await runPromise;
  assert.match(result.text, /Prior finding/);
  const updated = JSON.parse(readFileSync(runFile, 'utf8'));
  assert.ok(updated.waiting_gemini_sec >= 0.5, `expected waiting_gemini_sec >= 0.5, got ${updated.waiting_gemini_sec}`);
  assert.ok(Math.abs(updated.deadline - waiting.deadline - updated.waiting_gemini_sec) < 0.002,
    'deadline extends by exactly the paused duration');

  // Verify gpt_run timeout_sec schema, bounds validation, and forwarding.
  const runSchema = TOOLS.find((tool) => tool.name === 'gpt_run').inputSchema.properties.timeout_sec;
  assert.deepEqual(runSchema.type, ['number', 'null']);
  assert.equal(runSchema.minimum, 0);
  assert.equal(runSchema.maximum, undefined);
  for (const bad of [-1, NaN, Infinity, '60']) {
    await assert.rejects(handleTool('gpt_run', { goal: 'Check timeout', cwd: f.cwd, timeout_sec: bad }, f.env, f), /Invalid timeout_sec/);
  }
  const custom = await dispatchRun({ goal: 'Custom timeout run', cwd: f.cwd, timeout_sec: 9000, background: true }, f);
  assert.equal(custom.execution_timeout_sec, 9000);
  assert.ok(Math.abs(custom.deadline - Date.parse(custom.created_at) / 1000 - 9000) < 0.01);
  assert.equal((await waitForRun(custom.run_id, { ...f, timeoutSec: 5 })).state, 'finished');
});

test('unlimited defaults, explicit overrides and segmented timers preserve the entire budget', (t) => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: 1000 });
  assert.equal(validateExecTimeoutSec(undefined), undefined);
  for (const timeoutSec of [0, null]) {
    assert.equal(validateExecTimeoutSec(timeoutSec), null);
    assert.equal(deadlineForTimeout(timeoutSec), null);
    assert.equal(scheduleTimeout(() => assert.fail('unexpected timer'), timeoutSec), undefined);
    assert.equal(resolveExecTimeoutSec({ codex: { timeout_sec: 600, timeout_sec_customized: true }, timeoutSec }), null);
  }
  for (const legacy of [600, 3600]) {
    assert.equal(resolveExecTimeoutSec({ codex: { timeout_sec: legacy }, effort: 'high' }), null);
    assert.equal(resolveExecTimeoutSec({ codex: { timeout_sec: legacy, timeout_sec_customized: true } }), legacy);
  }
  assert.equal(resolveExecTimeoutSec(), null);
  const maxMs = 2 ** 31 - 1;
  let calls = 0;
  const seconds = (maxMs + 100) / 1000;
  const cancel = scheduleTimeout(() => assert.fail('cancelled timer fired'), seconds);
  scheduleTimeout(() => calls++, seconds);
  t.mock.timers.tick(maxMs);
  assert.equal(calls, 0);
  cancel();
  t.mock.timers.tick(99);
  assert.equal(calls, 0);
  t.mock.timers.tick(1);
  assert.equal(calls, 1);
});

test('execution clock survives 600s and early ask_gemini pauses before the start acknowledgement', async (t) => {
  const f = fixture(t);
  const controls = {};
  const packets = await appFixture(t, f, controls);
  const gateway = JSON.parse(readFileSync(f.configPath, 'utf8')).magpie.base_url;
  t.mock.timers.enable({ apis: ['Date', 'setTimeout', 'setInterval'], now: Date.now() });
  const start = (runId, prompt, timeoutSec) => {
    state(f, runId, { source: 'magpie_exec', pid: process.pid });
    return runAppServer({
      configPath: f.configPath, env: f.env, runFile: join(runDirectory(runId, f.sessionsDir), 'state.json'),
      sessionId: 'exec-clock-test', onIdentity() {}, prompt, cwd: f.cwd, model: 'codex/gpt-6.2-sol',
      effort: 'medium', sandbox: 'workspace-write', approvalPolicy: 'never', gatewayUrl: gateway, timeoutSec,
    });
  };
  const defaultId = 'a'.repeat(32);
  const defaultRun = assert.rejects(start(defaultId, 'Hold this turn', resolveExecTimeoutSec()), /interrupt/i);
  await until(() => readRun(defaultId, f.sessionsDir).turn_id);
  t.mock.timers.tick(601_000);
  assert.equal(readRun(defaultId, f.sessionsDir).state, 'running');
  assert.equal(packets.some(({ method }) => method === 'turn/interrupt'), false);
  t.mock.timers.tick(2_999_000);
  assert.equal(readRun(defaultId, f.sessionsDir).deadline, null);
  assert.equal(packets.some(({ method }) => method === 'turn/interrupt'), false);
  await interruptRun(defaultId, f);
  await defaultRun;

  const earlyId = 'b'.repeat(32);
  const earlyRun = assert.rejects(start(earlyId, 'Ask before turn acknowledgement', 30), /timed out after 30s/);
  await until(() => controls.ask);
  t.mock.timers.tick(5_000);
  controls.ask();
  const waiting = await until(() => {
    const record = readRun(earlyId, f.sessionsDir);
    return record.state === 'waiting_gemini' && record;
  });
  assert.equal(waiting.deadline - Date.now() / 1000, 25);
  t.mock.timers.tick(60_000);
  assert.equal(readRun(earlyId, f.sessionsDir).state, 'waiting_gemini');
  const reply = await runLiveOperation('reply', {
    run_id: earlyId, call_id: 'early_ask', message: 'Approved.',
  }, f);
  assert.equal(reply.ok, true);
  t.mock.timers.tick(100);
  const replyPacket = await until(() => packets.find(({ id }) => id === 'early-ask-rpc'));
  assert.equal(replyPacket.result.contentItems[0].text, 'Approved.');
  const resumed = readRun(earlyId, f.sessionsDir);
  assert.equal(resumed.waiting_gemini_sec, 60.1);
  assert.equal(Math.round(resumed.deadline * 1000) - Date.now(), 25_000);
  t.mock.timers.tick(24_999);
  assert.equal(readRun(earlyId, f.sessionsDir).state, 'running');
  assert.equal(packets.filter(({ method }) => method === 'turn/interrupt').length, 1);
  t.mock.timers.tick(1);
  await earlyRun;
});

test('Console migrates the legacy timeout and preserves a deliberately saved 600 seconds', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  const options = { ...f, root: f.cwd };
  const cfg = JSON.parse(readFileSync(f.configPath, 'utf8'));
  cfg.codex.timeout_sec = 600;
  cfg.limits = { consult_timeout_sec: 180 };
  writeFileSync(f.configPath, JSON.stringify(cfg));
  const migrated = await readState(f);
  assert.equal(migrated.codex_timeout_sec, null);
  assert.equal(migrated.consult_timeout_sec, null);
  assert.equal(resolveExecTimeoutSec({ codex: cfg.codex, effort: 'high' }), null);
  assert.equal(resolveExecTimeoutSec({ codex: { timeout_sec: 300 }, effort: 'high' }), 900);
  assert.equal(resolveExecTimeoutSec({ codex: cfg.codex, effort: 'high', timeoutSec: 30 }), 30);
  const run = await dispatchRun({ goal: 'Check migrated timeout', cwd: f.cwd, background: true }, f);
  assert.equal(run.execution_timeout_sec, null);
  assert.equal((await waitForRun(run.run_id, { ...f, timeoutSec: 5 })).execution_timeout_sec, null);
  assert.equal((await updateSettings({ work_split: 'balanced' }, f)).ok, true);
  const persisted = JSON.parse(readFileSync(f.configPath, 'utf8'));
  assert.equal(persisted.codex.timeout_sec, null);
  assert.equal(persisted.limits.consult_timeout_sec, null);
  const api = async (method, path, body) => dispatchPageApi({
    method, url: new URL(path, 'http://localhost'), rawPath: path.split('?', 1)[0], readBody: () => body,
  }, { ...options, appServer });
  const appServer = createAppServerManager({ ...options, observeOnly: true });
  t.after(() => appServer.stop());
  const saved = await api('POST', '/api/settings', {
    codex_timeout_sec: 600,
  });
  assert.equal(saved.status, 200);
  const overview = (await api('GET', '/api/overview')).body;
  assert.equal(overview.codex_timeout_sec, 600);
  for (const value of [180, 0, null]) {
    assert.equal((await api('POST', '/api/settings', { limits: { consult_timeout_sec: value } })).status, 200);
    const savedLimits = JSON.parse(readFileSync(f.configPath, 'utf8')).limits;
    assert.equal(savedLimits.consult_timeout_sec, value || null);
    assert.equal(savedLimits.consult_timeout_customized, true);
    assert.equal((await readState(f)).consult_timeout_sec, value || null);
  }
  assert.equal(JSON.parse(readFileSync(f.configPath, 'utf8')).codex.timeout_sec_customized, true);
  const customized = await dispatchRun({ goal: 'Check saved timeout', cwd: f.cwd, background: true }, f);
  assert.equal(customized.execution_timeout_sec, 600);
  assert.equal((await waitForRun(customized.run_id, { ...f, timeoutSec: 5 })).execution_timeout_sec, 600);
  assert.equal((await api('POST', '/api/settings', { codex_timeout_sec: -1 })).status, 400);
  assert.equal((await api('POST', '/api/tier', { tier: 'invalid' })).status, 400);
  const unsafeRestart = await api('POST', '/api/app-server/restart', { force: true });
  assert.equal(unsafeRestart.status, 409);
  const activity = await api('GET', '/api/requests?q=saved+timeout&limit=1');
  assert.equal(activity.body.items[0].run_id, customized.run_id);
  const details = await api('GET', `/api/requests/${encodeURIComponent(activity.body.items[0].id)}`);
  assert.equal(details.body.run_id, customized.run_id);
  const waiting = 'd'.repeat(32);
  state(f, waiting, { state: 'waiting_gemini', pending_tool_call: { tool: 'ask_gemini', call_id: 'ask_1' } });
  await assert.rejects(runLiveOperation('reply', { run_id: waiting, call_id: 'stale', message: 'yes' }, options), /not waiting/);
  await assert.rejects(runLiveOperation('reply', { run_id: waiting, call_id: 'ask_1', message: 'yes' },
    { ...options, root: join(f.root, 'other') }), /outside host workspace/);
});

test('remote_turn_unconfirmed blocks concurrent dispatch and worktree prune until interrupt clears it', async (t) => {
  const f = fixture(t);
  const subdir = initRepo(f.cwd);
  const packets = await appFixture(t, f);

  const first = await dispatchRun({ goal: 'Seed shared worktree', cwd: subdir, workspace: 'share', background: true }, f);
  const seeded = await waitForRun(first.run_id, { ...f, timeoutSec: 5 });
  assert.equal(seeded.state, 'finished', seeded.error);

  const failedId = '9'.repeat(32);
  state(f, failedId, {
    session_id: seeded.session_id, cwd: seeded.cwd, worktree_path: seeded.worktree_path, source_cwd: subdir,
    state: 'failed', codex_thread_id: seeded.codex_thread_id, turn_id: 'turn-unconfirmed',
    remote_turn_unconfirmed: true, unconfirmed_at: new Date().toISOString(),
    error: 'Remote Codex turn may still be running (no terminal status confirmed).',
    config_path: f.configPath,
  });

  // Recent unconfirmed record is occupying: blocks roster idle, same-session dispatch, and worktree prune.
  assert.equal(listActiveRoster({ ...f, sessionId: seeded.session_id }).length, 1);
  assert.equal(listActiveRoster({ ...f, sessionId: seeded.session_id })[0].remote_turn_unconfirmed, true);
  await assert.rejects(
    dispatchRun({ goal: 'Concurrent resume', cwd: subdir, session_id: seeded.session_id, background: true }, f),
    (err) => err.status === 409 && /unconfirmed remote turn/i.test(err.message),
  );
  assert.deepEqual(pruneShareWorktrees(f.cwd, { env: f.env, maxAgeMs: 0 }), { removed: [], kept: [seeded.worktree_path] });

  // gpt_ctl status exposes unconfirmed flag.
  const statusRes = await handleTool('gpt_ctl', { action: 'status', run_id: failedId }, f.env, f);
  assert.equal(JSON.parse(statusRes.content[0].text).unconfirmed, true);

  // gpt_ctl interrupt sends turn/interrupt and clears remote_turn_unconfirmed.
  const cleared = await interruptRun(failedId, f);
  assert.equal(cleared.ok, true);
  assert.equal(cleared.cleared_unconfirmed, true);
  assert.ok(packets.some(({ method, params }) => method === 'turn/interrupt' && params.turnId === 'turn-unconfirmed'));
  assert.equal(readRun(failedId, f.sessionsDir).remote_turn_unconfirmed, undefined);
  assert.equal(listActiveRoster({ ...f, sessionId: seeded.session_id }).length, 0);

  // Also clears when app-server reports the turn is no longer active.
  const goneId = '8'.repeat(32);
  state(f, goneId, {
    session_id: seeded.session_id, cwd: seeded.cwd, state: 'outcome_unknown',
    codex_thread_id: seeded.codex_thread_id, turn_id: 'turn-gone',
    remote_turn_unconfirmed: true, unconfirmed_at: new Date().toISOString(), config_path: f.configPath,
  });
  const clearedGone = await interruptRun(goneId, {
    ...f,
    connect: async () => ({
      disconnected: false,
      request: async () => { throw new Error('Turn not found'); },
      close() {},
    }),
  });
  assert.equal(clearedGone.cleared_unconfirmed, true);
  const gone = readRun(goneId, f.sessionsDir);
  assert.equal(gone.remote_turn_unconfirmed, undefined);
  assert.equal(gone.unconfirmed_at, undefined);
  assert.equal(gone.state, 'failed');
  assert.equal(gone.progress.phase, 'failed');
  assert.ok(gone.finished_at);
  assert.equal(gone.pending_tool_call, null);

  // Ancient unconfirmed run past hold window does not block forever.
  const oldIso = new Date(Date.now() - 45 * 60 * 1000).toISOString();
  assert.equal(isRecentUnconfirmedRun({
    state: 'failed', remote_turn_unconfirmed: true, unconfirmed_at: oldIso, deadline: Date.now() / 1000 - 300,
  }), false);

  // After clearing, same-session dispatch succeeds.
  const resumed = await dispatchRun({ goal: 'Resume after clear', cwd: subdir, session_id: seeded.session_id, background: true }, f);
  assert.equal((await waitForRun(resumed.run_id, { ...f, timeoutSec: 5 })).state, 'finished');
});

test('manual interrupt settles missing identity, ACK and observed terminal outcomes for an unknown run', async (t) => {
  const f = fixture(t);
  const id = 'd'.repeat(32);
  for (const [confirmation, expected] of [['missing', 'failed'], ['ack', 'outcome_unknown'],
    ['interrupted', 'interrupted'], ['failed', 'failed'], ['completed', 'finished']]) {
    state(f, id, { state: 'outcome_unknown', remote_turn_unconfirmed: true, unconfirmed_at: 'old',
      progress: { thread_id: 'th', turn_id: confirmation === 'missing' ? null : 'tu' }, pending_tool_call: {} });
    const cleared = await interruptRun(id, { ...f, confirmMs: 0, getState: () => ({ status: 'running' }),
      connect: async (_, onEvent) => ({
        request: async (_, { threadId, turnId }) => {
          assert.notEqual(confirmation, 'missing');
          if (confirmation === 'ack') return {};
          onEvent({ method: 'turn/completed', params: { threadId, turn: { id: turnId, status: confirmation } } });
          throw new Error('Socket closed after event');
        },
        close() {},
      }),
    });
    if (confirmation === 'ack') {
      assert.equal(readRun(id, f.sessionsDir).remote_turn_unconfirmed, true);
      continue;
    }
    assert.equal(cleared.cleared_unconfirmed, true);
    const settled = readRun(id, f.sessionsDir);
    assert.equal(settled.state, expected, confirmation);
    assert.equal(settled.progress.phase, expected);
    assert.equal(settled.pending_tool_call, null);
    assert.ok(settled.finished_at);
    assert.equal(settled.remote_turn_unconfirmed, undefined);
    assert.equal(settled.unconfirmed_at, undefined);
  }
});

test('pre-connect app-server failure does not leave an unbound session without codex_thread_id', async (t) => {
  const f = fixture(t);
  await appFixture(t, f);
  // Seed one valid session first to verify resume failure also does not corrupt it.
  const okRun = await dispatchRun({ goal: 'Healthy seed run', cwd: f.cwd, background: true }, f);
  const okDone = await waitForRun(okRun.run_id, { ...f, timeoutSec: 5 });
  assert.equal(okDone.state, 'finished', okDone.error);
  const seedFile = join(f.sessionsDir, `${okDone.session_id}.json`);
  const seedBefore = readFileSync(seedFile, 'utf8');

  // Stop app-server state so connectAppServer fails before thread/start.
  writeFileSync(f.env.GEMPT_APP_SERVER_STATE, JSON.stringify({ status: 'stopped' }));
  const broken = await dispatchRun({ goal: 'Connect failure new session', cwd: f.cwd, background: true }, f);
  const failed = await waitForRun(broken.run_id, { ...f, timeoutSec: 5 });
  assert.equal(failed.state, 'failed');
  assert.equal(existsSync(join(f.sessionsDir, `${broken.session_id}.json`)), false);

  const brokenResume = await dispatchRun({ goal: 'Connect failure resume', cwd: f.cwd, session_id: okDone.session_id, background: true }, f);
  const failedResume = await waitForRun(brokenResume.run_id, { ...f, timeoutSec: 5 });
  assert.equal(failedResume.state, 'failed');
  assert.equal(readFileSync(seedFile, 'utf8'), seedBefore);
});

test('pruning a clean shared worktree syncs session cwd back to main repo for future resumes', async (t) => {
  const f = fixture(t);
  const subdir = initRepo(f.cwd);
  await appFixture(t, f);

  const shared = await dispatchRun({ goal: 'Work in isolated worktree', cwd: subdir, workspace: 'share', background: true }, f);
  const done = await waitForRun(shared.run_id, { ...f, timeoutSec: 5 });
  assert.equal(done.state, 'finished', done.error);
  const sessionFile = join(f.sessionsDir, `${done.session_id}.json`);
  const beforePrune = JSON.parse(readFileSync(sessionFile, 'utf8'));
  assert.equal(beforePrune.worktree_path, done.worktree_path);
  assert.equal(beforePrune.cwd, join(done.worktree_path, 'project'));

  assert.deepEqual(pruneShareWorktrees(f.cwd, { env: f.env, maxAgeMs: 0 }), { removed: [done.worktree_path], kept: [] });
  assert.equal(existsSync(done.worktree_path), false);
  const afterPrune = JSON.parse(readFileSync(sessionFile, 'utf8'));
  assert.equal(afterPrune.cwd, subdir);
  assert.equal(afterPrune.worktree_path, null);
  assert.ok(afterPrune.worktree_pruned_at);
  assert.equal(afterPrune.pruned_worktree_path, done.worktree_path);

  // Resume with workspace='inherit' in main repo subdir succeeds.
  const resumedInherit = await dispatchRun({ goal: 'Continue in main repo', cwd: subdir, session_id: done.session_id,
    workspace: 'inherit', background: true }, f);
  const doneInherit = await waitForRun(resumedInherit.run_id, { ...f, timeoutSec: 5 });
  assert.equal(doneInherit.state, 'finished', doneInherit.error);
  assert.equal(doneInherit.cwd, subdir);

  // Resume with workspace='share' recreates worktree and succeeds.
  const resumedShare = await dispatchRun({ goal: 'Continue in fresh worktree', cwd: subdir, session_id: done.session_id,
    workspace: 'share', background: true }, f);
  const doneShare = await waitForRun(resumedShare.run_id, { ...f, timeoutSec: 5 });
  assert.equal(doneShare.state, 'finished', doneShare.error);
  assert.equal(doneShare.worktree_path, done.worktree_path);
});

test('gempt-wait batch returns non-zero when first-ready item fails and session_id validation enforces strict rules', (t) => {
  const f = fixture(t);
  const waitScript = fileURLToPath(new URL('../core/wait.mjs', import.meta.url));
  const failedId = '1'.repeat(32);
  const unknownId = '2'.repeat(32);
  const runningId = '3'.repeat(32);
  const waitingId = '4'.repeat(32);
  const finishedId = '5'.repeat(32);
  state(f, failedId, { state: 'failed', error: 'boom' });
  state(f, unknownId, { state: 'outcome_unknown' });
  state(f, runningId, { state: 'running' });
  state(f, waitingId, { state: 'waiting_gemini' });
  state(f, finishedId, { state: 'finished' });

  const batchFailed = spawnSync(process.execPath, [waitScript, `${failedId},${runningId}`], { env: f.env, encoding: 'utf8' });
  assert.equal(batchFailed.status, 1, batchFailed.stderr);
  assert.deepEqual(JSON.parse(batchFailed.stdout).ready, [failedId]);

  const batchUnknown = spawnSync(process.execPath, [waitScript, `${unknownId},${runningId}`], { env: f.env, encoding: 'utf8' });
  assert.equal(batchUnknown.status, 1, batchUnknown.stderr);
  assert.deepEqual(JSON.parse(batchUnknown.stdout).ready, [unknownId]);

  const batchFinished = spawnSync(process.execPath, [waitScript, `${finishedId},${runningId}`], { env: f.env, encoding: 'utf8' });
  assert.equal(batchFinished.status, 0, batchFinished.stderr);
  assert.deepEqual(JSON.parse(batchFinished.stdout).ready, [finishedId]);

  const singleWaiting = spawnSync(process.execPath, [waitScript, waitingId], { env: f.env, encoding: 'utf8' });
  assert.equal(singleWaiting.status, 0, singleWaiting.stderr);
  assert.equal(JSON.parse(singleWaiting.stdout).state, 'waiting_gemini');

  for (const good of ['exec-20261123-abc123', 'consult-1.2_3', 'session-a', 'a'.repeat(128)]) {
    assert.equal(isValidSessionId(good), true, good);
    assert.equal(validateSessionId(` ${good} `), good);
  }
  for (const bad of ['', '.', '..', '.hidden', '-flag', 'exec-a..b', 'exec-end.', 'a'.repeat(129), 'CON', 'nul.txt', '../escape', 'bad/id', 'bad\\id']) {
    assert.equal(isValidSessionId(bad), false, bad);
    assert.throws(() => validateSessionId(bad), /Invalid session_id/);
  }
});

test('CLI prints exact usage and accepts status selectors and all reply forms', (t) => {
  const f = fixture(t);
  const cli = (args, input) => spawnSync(process.execPath, [cliPath, ...args], {
    env: f.env, encoding: 'utf8', timeout: 5000, input,
  });
  assert.match(cli(['-h']).stdout, /^Usage: gempt </);
  for (const command of ['install-agy', 'run', 'ask', 'mcp', 'discover', 'status', 'init', 'tier',
    'sandbox', 'effort', 'steer', 'interrupt', 'reply-tool', 'prune-worktrees', 'print-mcp', 'board', 'swarm', 'wait']) {
    for (const flag of ['--help', '-h']) {
      const result = cli([command, flag]);
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.stdout, new RegExp(`^Usage: gempt ${command}(?: |$)`));
    }
    const bad = cli([command, '--unrecognized-option']);
    assert.equal(bad.status, 1);
    assert.ok(bad.stderr.includes(cli([command, '-h']).stdout.trim()), bad.stderr);
  }
  const badBoard = cli(['board', 'read', 'extra']);
  assert.equal(badBoard.status, 1);
  assert.match(badBoard.stderr, /Usage: gempt board read/);
  assert.match(cli(['swarm', 'add', '-h']).stdout, /Usage: gempt swarm add NAME/);

  const runId = 'a'.repeat(32);
  const call = { tool: 'ask_gemini', call_id: 'call_a' };
  state(f, runId, { state: 'waiting_gemini', pending_tool_call: call });
  const status = cli(['status', runId]);
  assert.equal(status.status, 0, status.stderr);
  assert.deepEqual(JSON.parse(status.stdout), JSON.parse(cli(['status', '--run-id', runId]).stdout));
  assert.equal(JSON.parse(status.stdout).reply_command, `gempt reply-tool ${runId} call_a "<answer>"`);
  assert.match(cli(['status', runId, '--run-id', runId]).stderr, /Usage: gempt status/);

  for (const [index, [args, input, expected]] of [
    [['call_a', 'plain answer'], undefined, 'plain answer'],
    [['call_a', '--text', 'flag answer'], undefined, 'flag answer'],
    [['call_a', '--message', 'alias answer'], undefined, 'alias answer'],
    [['call_a', '-'], 'line 1\nline 2\n', 'line 1\nline 2\n'],
    [['call_a', '--text', '-'], '多行\n\n', '多行\n\n'],
    [['--text', 'only pending call'], undefined, 'only pending call'],
  ].entries()) {
    const id = (index + 1).toString(16).repeat(32);
    state(f, id, { state: 'waiting_gemini', pending_tool_call: call });
    const reply = cli(['reply-tool', id, ...args], input);
    assert.equal(reply.status, 0, reply.stderr);
    assert.equal(JSON.parse(reply.stdout).call_id, 'call_a');
    assert.deepEqual(JSON.parse(readFileSync(join(runDirectory(id, f.sessionsDir), 'tool_replies', 'call_a.json'))),
      { answer: expected, success: true });
  }
  const secondCall = { ...call, call_id: 'call_b' };
  state(f, runId, { state: 'waiting_gemini', pending_tool_call: call, pending_tool_calls: [call, secondCall] });
  const ambiguous = cli(['reply-tool', runId, '--text', 'Which call?']);
  assert.equal(ambiguous.status, 1);
  assert.match(ambiguous.stderr, /multiple pending tool calls \(call_a, call_b\).*\nUsage: gempt reply-tool/);
  assert.equal(cli(['reply-tool', runId, 'call_b', '--text', 'Second reply']).status, 0);
  assert.match(cli(['reply-tool', runId, 'call_a']).stderr, /Expected a reply body.*\nUsage: gempt reply-tool/);
  state(f, runId, { state: 'finished' });
  assert.match(cli(['reply-tool', runId, '--text', 'Late']).stderr, /no pending.*\nUsage: gempt reply-tool/);
});

test('wait surfaces reply commands for a single run and returns early for pending asks in a batch', (t) => {
  const f = fixture(t);
  const asking = 'a'.repeat(32), running = 'b'.repeat(32);
  const call = { tool: 'ask_gemini', call_id: 'ask_1', arguments: { question: 'Proceed?' } };
  state(f, asking, { state: 'waiting_gemini', pending_tool_call: call });
  state(f, running, { state: 'running' });
  const wait = (...args) => jsonProcess(process.execPath, [cliPath, 'wait', ...args], { env: f.env, timeout: 2000 });
  const expected = `gempt reply-tool ${asking} ask_1 "<answer>"`;
  assert.equal(wait(asking).reply_command, expected);
  for (const options of [[], ['--all']]) {
    const batch = wait(...options, `${asking},${running}`);
    assert.deepEqual(batch.ready, [asking]);
    assert.deepEqual(batch.active, [running]);
    assert.equal(batch.runs[asking].reply_command, expected);
  }
});

test('wait receipts preserve full text, changes, commands and failed verification output', (t) => {
  const f = fixture(t);
  const id = 'a'.repeat(32);
  const text = '检查😀'.repeat(3000);
  const files = Array.from({ length: 31 }, (_, i) => 'M file-' + i);
  const commands = Array.from({ length: 7 }, (_, i) => ({ command: i + text, exit_code: 1 }));
  state(f, id, { state: 'finished', goal: text, text, before: { status: '' }, after: { status: files.join('\n') },
    progress: { last_message: text, recent_commands: commands },
    result: { text, verification: { status: 'failed', checks: [{ command: 'verify', exit_code: 1, output: text }] } } });
  const receipt = jsonProcess(process.execPath, [cliPath, 'wait', id], { env: f.env });
  assert.deepEqual([receipt.text, receipt.goal, receipt.progress.last_message, receipt.result.text], Array(4).fill(text));
  assert.deepEqual(receipt.changed_files, files);
  assert.deepEqual(receipt.progress.recent_commands, commands);
  assert.equal(receipt.result.verification.checks[0].output_tail, text);
});

test('wait keeps heartbeats on stderr and uses journal, progress and state activity before stalling', async (t) => {
  const f = fixture(t);
  const id = 'd'.repeat(32);
  const file = join(runDirectory(id, f.sessionsDir), 'state.json');
  const journal = join(dirname(file), 'events.jsonl');
  const stale = new Date(Date.now() - 600_000);
  for (const signal of ['journal', 'progress', 'state', 'reconnect']) {
    const now = new Date();
    state(f, id, { progress: { phase: 'testing', updated_at: (signal === 'progress' ? now : stale).toISOString(),
      ...(signal === 'journal' ? {} : { recent_commands: [{ command: 'npm test' }] }) } });
    if (signal !== 'state') utimesSync(file, stale, stale);
    writeFileSync(journal, JSON.stringify({ method: 'item/started', params: { item: { command: 'npm test' } } }) + '\n');
    if (signal === 'reconnect') appendFileSync(journal, JSON.stringify({
      method: 'error', params: { error: { message: 'Reconnecting to server…' }, willRetry: true },
    }) + '\n');
    if (signal !== 'journal') utimesSync(journal, stale, stale);
    const child = spawn(process.execPath, [cliPath, 'wait', id, '--heartbeat-sec', '.02',
      ...(signal === 'reconnect' ? [] : ['--stale-sec', '5'])], {
      env: f.env, stdio: ['ignore', 'pipe', 'pipe'], timeout: 8000,
    });
    let stdout = '', stderr = '', finished = false;
    child.stdout.setEncoding('utf8').on('data', (chunk) => { stdout += chunk; });
    child.stderr.setEncoding('utf8').on('data', (chunk) => {
      stderr += chunk;
      if (!finished && /last_cmd="npm test"/.test(stderr)) {
        finished = true;
        state(f, id, { state: 'finished' });
      }
    });
    const writer = signal === 'journal'
      ? setInterval(() => appendFileSync(journal, '{"method":"item/commandExecution/outputDelta"}\n'), 30) : null;
    try {
      assert.deepEqual(await once(child, 'close'), [0, null], stderr);
    } finally {
      clearInterval(writer);
    }
    assert.equal(JSON.parse(stdout).state, 'finished');
    if (signal === 'reconnect') assert.match(stderr, /error="Reconnecting to server…" · updated \d+s ago/);
    else assert.match(stderr, /^\[gempt-wait \d+m\d+s\] .*running · phase=testing · last_cmd="npm test" · updated \d+s ago/m, signal);
  }

  const wait = (...options) => spawnSync(process.execPath, [cliPath, 'wait', id, '--stale-sec', '.5', '--heartbeat-sec', '0',
    ...options], { env: f.env, encoding: 'utf8', timeout: 2000 });
  utimesSync(journal, stale, stale);
  state(f, id, { progress: { updated_at: stale.toISOString() } });
  utimesSync(file, stale, stale);
  const stalled = wait();
  assert.equal(stalled.status, 1, stalled.stderr);
  assert.equal(stalled.stderr, '');
  const { state: active, stalled: stoppedWaiting, stale_sec, reason } = JSON.parse(stalled.stdout);
  assert.deepEqual([active, stoppedWaiting, stale_sec, reason], ['running', true, .5, 'no_progress']);

  state(f, id, { source: 'magpie_exec', pid: 2147483000, cli: 'claude' });
  const exited = wait('--stale-sec', '300');
  assert.equal(exited.status, 1, exited.stderr);
  assert.equal(JSON.parse(exited.stdout).reason, 'worker_pid_exited');
  assert.equal(JSON.parse(exited.stdout).stalled, true);
  assert.equal(readRun(id, f.sessionsDir).state, 'failed');
});

test('board wait surfaces live tool and board replies after the message cursor', async (t) => {
  const f = fixture(t);
  const swarmId = 'reply-board';
  const run_id = 'c'.repeat(32);
  const ask = { tool: 'ask_gemini', call_id: 'ask_1', arguments: { question: 'Proceed with the release?' } };
  await ensureSwarm({ swarmId, cwd: f.cwd, sessionsDir: f.sessionsDir });
  await upsertWorker(swarmId, { name: 'alpha', state: 'working', run_id }, f);
  state(f, run_id, { state: 'waiting_gemini', pending_tool_call: ask });
  const post = await postBoardMessage(swarmId, { from: 'alpha', kind: 'question',
    text: '@coordinator Proceed with the release?', meta: { run_id, call_id: ask.call_id } }, f);
  const expected = `gempt reply-tool ${run_id} ask_1 "<answer>"`;
  const first = await waitForBoard(swarmId, { ...f, timeoutMs: 1000 });
  assert.equal(first.messages[0].reply_command, expected);
  const after = await waitForBoard(swarmId, { ...f, since: post.seq, timeoutMs: 1000 });
  assert.deepEqual(after.messages, []);
  assert.deepEqual(after.pending_questions, [{ agent: 'alpha', run_id, state: 'waiting_gemini',
    call_id: 'ask_1', question: 'Proceed with the release?', reply_command: expected }]);
  assert.equal(after.roster[0].reply_command, expected);

  state(f, run_id, { state: 'finished' });
  await upsertWorker(swarmId, { name: 'beta', state: 'working' }, f);
  const question = await handleBoardMcpTool('board_ask', { question: 'Which scope?' }, f.env,
    { ...f, swarmId, agentName: 'beta' });
  const { message } = JSON.parse(question.content[0].text);
  const board = await waitForBoard(swarmId, { ...f, since: message.seq, timeoutMs: 1000 });
  assert.deepEqual(board.pending_questions, [{ agent: 'beta', state: 'waiting_reply', seq: message.seq,
    question: '@coordinator Which scope?', reply_command:
      `gempt board post --swarm ${swarmId} --from coordinator --channel main --reply-to ${message.seq} --text "@beta <answer>"` }]);
  const reply = await execFileAsync(process.execPath, [cliPath, 'board', 'post', '--swarm', swarmId, '--from', 'coordinator',
    '--channel', 'main', '--reply-to', String(message.seq), '--text', '@beta Scope A.'], { env: f.env });
  assert.equal(JSON.parse(reply.stdout).message.reply_to_seq, message.seq);
});

test('board wait emits active worker and head on stderr and detects silent and dead workers', async (t) => {
  const f = fixture(t);
  const swarmId = 'board-watchdog';
  const run_id = 'b'.repeat(32);
  state(f, run_id, { progress: { phase: 'testing' } });
  await upsertWorker(swarmId, { name: 'alpha', state: 'working', run_id }, f);
  const child = spawn(process.execPath, [cliPath, 'wait', '--board', swarmId, '--heartbeat-sec', '.02', '--stale-sec', '.5'],
    { env: f.env, stdio: ['ignore', 'pipe', 'pipe'], timeout: 5000 });
  let stdout = '', stderr = '';
  child.stdout.setEncoding('utf8').on('data', (chunk) => { stdout += chunk; });
  child.stderr.setEncoding('utf8').on('data', (chunk) => { stderr += chunk; });
  assert.deepEqual(await once(child, 'close'), [1, null], stderr);
  assert.match(stderr, /^\[gempt-wait \d+m\d+s\] board head=#0 · alpha running · phase=testing/m);
  const stalled = JSON.parse(stdout);
  assert.equal(stalled.reason, 'no_progress');
  assert.equal(stalled.roster[0].stalled, true);

  state(f, run_id, { source: 'magpie_exec', pid: 2147483000, cli: 'claude' });
  const exited = await waitForBoard(swarmId, { ...f, timeoutMs: 1000 });
  assert.equal(exited.stalled, true);
  assert.equal(exited.reason, 'worker_pid_exited');
  assert.equal(exited.roster[0].state, 'stopped');
});

test('runAppServer interrupt confirmation window distinguishes confirmed vs unconfirmed timeout', async (t) => {
  const f = fixture(t);
  let delayInterruptMs = 80;
  let emitCompletedOnInterrupt = true;
  const server = createServer((req, res) => { res.statusCode = 404; res.end('{}'); });
  const sockets = new Set();
  server.on('upgrade', (req, socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    const accept = createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
    const frame = (opcode, body) => {
      const header = body.length < 126 ? Buffer.from([0x80 | opcode, body.length])
        : Buffer.from([0x80 | opcode, 126, body.length >> 8, body.length & 255]);
      socket.write(Buffer.concat([header, body]));
    };
    const send = (data) => frame(1, Buffer.from(JSON.stringify({ jsonrpc: '2.0', ...data })));
    let buffer = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 2) {
        const opcode = buffer[0] & 15;
        let length = buffer[1] & 127;
        const offset = length === 126 ? 4 : 2;
        if (buffer.length < offset) return;
        if (offset === 4) length = buffer.readUInt16BE(2);
        if (buffer.length < offset + 4 + length) return;
        const mask = buffer.subarray(offset, offset + 4);
        const body = buffer.subarray(offset + 4, offset + 4 + length);
        for (let i = 0; i < body.length; i++) body[i] ^= mask[i % 4];
        buffer = buffer.subarray(offset + 4 + length);
        if (opcode === 8) { frame(8, body); socket.end(); continue; }
        if (opcode !== 1) continue;
        const { method, id, params } = JSON.parse(body.toString('utf8'));
        if (method === 'initialized') continue;
        if (method === 'initialize') send({ id, result: {} });
        else if (method === 'thread/start') {
          send({ id, result: { thread: { id: 'th-delay' }, cwd: params.cwd, modelProvider: params.modelProvider, sandbox: { type: 'workspaceWrite' } } });
        } else if (method === 'turn/start') {
          send({ method: 'turn/started', params: { threadId: params.threadId, turnId: 'tu-delay', turn: { id: 'tu-delay', status: 'inProgress' } } });
          send({ id, result: { turn: { id: 'tu-delay' } } });
        } else if (method === 'turn/interrupt') {
          send({ id, result: {} });
          if (emitCompletedOnInterrupt) {
            setTimeout(() => {
              if (!socket.destroyed) {
                send({ method: 'turn/completed', params: { threadId: params.threadId, turnId: params.turnId, turn: { id: params.turnId, status: 'interrupted' } } });
              }
            }, delayInterruptMs);
          }
        }
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => {
    for (const s of sockets) s.destroy();
    return new Promise((resolve) => server.close(resolve));
  });
  const addr = `127.0.0.1:${server.address().port}`;
  const gateway = `http://${addr}`;
  f.env.GEMPT_APP_SERVER_STATE = join(f.root, 'delay-app-server.json');
  writeFileSync(f.env.GEMPT_APP_SERVER_STATE, JSON.stringify({
    status: 'running', ws_url: `ws://${addr}`, auth_header: 'Authorization', auth_token: 'tok', gateway_url: gateway,
  }));

  const id1 = '6'.repeat(32);
  const file1 = join(runDirectory(id1, f.sessionsDir), 'state.json');
  saveRun(file1, { source: 'magpie_exec', run_id: id1, session_id: 'exec-delay-1', cwd: f.cwd, state: 'running', pid: process.pid });
  await assert.rejects(
    runAppServer({
      configPath: f.configPath, env: f.env, runFile: file1, sessionId: 'exec-delay-1', onIdentity() {},
      prompt: 'Hold', cwd: f.cwd, model: 'codex/gpt-6.2-sol', effort: 'medium', sandbox: 'workspace-write',
      approvalPolicy: 'never', gatewayUrl: gateway, timeoutSec: 0.12, interruptConfirmMs: 400,
    }),
    (err) => /timed out/.test(err.message) && !err.remoteTurnUnconfirmed,
  );

  emitCompletedOnInterrupt = false;
  const id2 = '7'.repeat(32);
  const file2 = join(runDirectory(id2, f.sessionsDir), 'state.json');
  saveRun(file2, { source: 'magpie_exec', run_id: id2, session_id: 'exec-delay-2', cwd: f.cwd, state: 'running', pid: process.pid });
  await assert.rejects(
    runAppServer({
      configPath: f.configPath, env: f.env, runFile: file2, sessionId: 'exec-delay-2', onIdentity() {},
      prompt: 'Hold forever', cwd: f.cwd, model: 'codex/gpt-6.2-sol', effort: 'medium', sandbox: 'workspace-write',
      approvalPolicy: 'never', gatewayUrl: gateway, timeoutSec: 0.12, interruptConfirmMs: 120,
    }),
    (err) => err.remoteTurnUnconfirmed === true && /Remote Codex turn may still be running/.test(err.message),
  );
});

test('Live restricts out-of-workspace files, exposes remote_turn_unconfirmed, and manual workers use the shared work split', async (t) => {
  const f = fixture(t);
  const inside = join(f.cwd, 'ok.txt');
  const outside = join(f.root, 'secret.txt');
  writeFileSync(inside, 'ok\n');
  writeFileSync(outside, 'secret\n');
  const id = '5'.repeat(32);
  state(f, id, {
    state: 'failed', codex_thread_id: 'th-1', turn_id: 'tu-1', remote_turn_unconfirmed: true,
    progress: { phase: 'failed', touched_files: ['ok.txt', '../secret.txt', outside] },
  });
  const streamed = await runLiveOperation('stream', { run_id: id }, { ...f, root: f.cwd });
  assert.equal(streamed.run.remote_turn_unconfirmed, true);
  assert.equal(streamed.run.can_interrupt, true);
  assert.ok(streamed.file_uris['ok.txt']);
  assert.equal(streamed.file_uris['../secret.txt'], undefined);
  assert.equal(streamed.file_uris[outside], undefined);
  assert.equal(streamed.restricted_files['../secret.txt'], 'outside_workspace');
  assert.equal(streamed.restricted_files[outside], 'outside_workspace');

  mkdirSync(dirname(f.configPath), { recursive: true });
  writeFileSync(join(dirname(f.configPath), 'app-server.json'),
    JSON.stringify({ status: 'running', pid: process.pid, ws_url: 'ws://127.0.0.1:1' }));
  writeFileSync(f.configPath, JSON.stringify({ routing_policy: { work_split: 'balanced', swarm_split: 'gpt_first' } }));
  const env = { ...f.env, GEMPT_APP_SERVER_STATE: join(dirname(f.configPath), 'app-server.json') };
  const swarmStart = jsonProcess(process.execPath, [hookPath], {
    env: { ...env, GEMPT_SWARM_MODE: '1' },
    input: JSON.stringify({ hookEventName: 'SessionStart', conversationId: 'swarm-worker', workspacePaths: [f.cwd] }),
  });
  assert.match(swarmStart.injectSteps[0].systemMessage.systemMessage, /35% GPT/);
});

test('hook uses the auto-selected runner and allows manual Swarm workers and user bypass', (t) => {
  const f = fixture(t);
  mkdirSync(dirname(f.configPath));
  writeFileSync(f.configPath, JSON.stringify({
    execution: { default_cli: 'auto', cli_paths: { opencode: process.execPath } },
    routing_policy: { work_split: 'gpt_first', default_tier: 'sonnet', native_swarm: true },
    tiers: { sonnet: { exec_model: 'vendor/custom' } },
  }));
  const invoke = (toolCall, fields) => jsonProcess(process.execPath, [hookPath], { env: f.env,
    input: JSON.stringify({ hookEventName: 'PreToolUse', conversationId: 'coordinator', toolCall, ...fields }) });
  const edit = { name: 'edit_file', args: { Path: join(f.cwd, 'index.mjs') } };
  assert.match(invoke(edit).reason, /80% Write Gate/);
  assert.equal(invoke(edit, { lastUserInput: '自己改' }).decision, 'allow');
  for (const ToolName of ['gpt_run', 'gpt_swarm', 'gpt_board']) {
    assert.equal(invoke({ name: 'call_mcp_tool', args: { ServerName: 'gempt_gempt', ToolName, Arguments: { action: 'add' } } }).decision, 'allow');
  }
  const read = { name: 'read_file', args: { Path: join(f.cwd, 'index.mjs') } };
  const worker = { conversationId: 'worker', parentConversationId: 'coordinator' };
  for (let i = 0; i < 2; i++) assert.equal(invoke(read, worker).decision, 'allow');
  assert.equal(invoke(read, worker).decision, 'deny');
  assert.equal(invoke({ name: 'run_command', args: { CommandLine: 'cat work.txt | gempt run --goal -' } },
    { conversationId: 'worker' }).decision, 'allow');
});

test('packaging guards unsafe output paths and rolls back in-place sync on verify failure', async (t) => {
  const f = fixture(t);
  assert.throws(() => assertSafeOutDir(f.root, { cwd: f.root, repoRoot: f.root, homeDir: f.root, tmpDir: f.root }), /Refusing/);
  const staged = join(f.root, 'staged');
  const target = join(f.root, 'target');
  mkdirSync(staged);
  mkdirSync(target);
  writeFileSync(join(target, 'keep.txt'), 'original');
  writeFileSync(join(staged, 'keep.txt'), 'updated');
  writeFileSync(join(staged, 'added.txt'), 'extra');
  await assert.rejects(
    syncInPlace(staged, target, { verify: async () => { throw new Error('verify boom'); } }),
    /verify boom/,
  );
  assert.equal(readFileSync(join(target, 'keep.txt'), 'utf8'), 'original');
  assert.equal(existsSync(join(target, 'added.txt')), false);
});

test('Console API saves nested preferences and supports gateway actions', async (t) => {
  const f = fixture(t);
  const gateway = createServer((_req, res) => res.end(JSON.stringify({ data: [{ id: 'vendor/model:small' }] })));
  gateway.listen(0, '127.0.0.1');
  await once(gateway, 'listening');
  t.after(() => gateway.close());
  const url = 'http://127.0.0.1:' + gateway.address().port;
  const set = (args) => updateSettings(args, f);
  const secret = 'mcp-fixture-value-z123';
  assert.equal((await set({ ui: { language: 'zh' } })).ok, true);
  assert.equal((await readOverview(f)).configured, false);
  assert.equal(resetSettings({}, f).keep_gateway, true);
  const saved = await set({
    magpie: { base_url: url + '/', api_key: secret, api_key_env: '' },
    routing_policy: { excluded_models: ['  model-to-skip  '] },
    utility: { model: 'vendor/model:small', reasoning_effort: 'high' },
    limits: { consult_timeout_sec: 18000.5 },
    codex: { cli_path: '', extra_args: [' --strict-config '] },
    ui: { language: 'en' },
  });
  assert.equal(saved.ok, true);
  const state = await readOverview(f);
  assert.equal(state.magpie_base_url, url);
  assert.equal(state.magpie_api_key_hint, '••••z123');
  assert.equal(state.magpie_api_key_env, '');
  assert.equal(state.utility.model, 'vendor/model:small');
  assert.equal(state.utility.reasoning_effort, 'high');
  assert.deepEqual(state.excluded_models, ['model-to-skip']);
  assert.deepEqual(state.codex_extra_args, ['--strict-config']);
  assert.equal(state.consult_timeout_sec, 18000.5);
  assert.equal(state.ui.language, 'en');
  assert.ok(!JSON.stringify(state).includes(secret));
  assert.equal(readRoutingSummary(f).ui.language, 'en');
  assert.equal((await set({ swarm_shell_model: 'shell bad' })).ok, false);
  assert.equal((await set({ codex: { extra_args: [1] } })).ok, false);
  assert.equal((await set({ limits: { consult_timeout_sec: -1 } })).ok, false);
  assert.equal((await set({ ui: { language: 'de' } })).ok, false);
  assert.equal((await set({ codex: { extra_args: ['bad\nflag'] } })).ok, false);
  const probe = await probeGatewaySettings({ base_url: url, api_key: 'draft-secret' }, f);
  assert.equal(probe.reachable, true);
  assert.deepEqual(probe.models, ['vendor/model:small']);
  assert.ok(!JSON.stringify(probe).includes('draft-secret'));
  resetSettings({ keep_gateway: true }, f);
  assert.equal(JSON.parse(readFileSync(f.configPath, 'utf8')).magpie.api_key, secret);
  resetSettings({ keep_gateway: false }, f);
  assert.equal(JSON.parse(readFileSync(f.configPath, 'utf8')).magpie.api_key, '');
});

test('legacy Swarm config routes ordinary gpt_run to the implicit board, preserves receipts, resumes and forks', async (t) => {
  const f = fixture(t);
  const controls = {};
  const packets = await appFixture(t, f, controls);
  const cfg = JSON.parse(readFileSync(f.configPath));
  const retired = { native_swarm: true, swarm_shell_model: 'old/shell', swarm_split: 'gemini' };
  Object.assign(cfg.routing_policy, retired);
  writeFileSync(f.configPath, JSON.stringify(cfg));
  const context = { cwd: f.cwd, host_conversation_id: 'conversation-A' };
  const run = async (goal, session_id) => {
    const res = await handleTool('gpt_run', { ...context, cli: 'codex', goal, session_id, background: true }, f.env, f);
    const receipt = JSON.parse(res.content[0].text);
    assert.equal(receipt.wait_command, buildWaitCommand(receipt.run_id, f));
    assert.ok(receipt.session_id);
    return receipt;
  };
  const [a, b] = await Promise.all([run('Hold this turn'), run('Hold this turn too')]);
  const status = async () => {
    const receipt = await handleSwarmAction({ ...context, action: 'status' }, f);
    assert.equal(receipt.roster.find(({ role }) => role === 'coordinator').conversation_id, context.host_conversation_id);
    return { ...receipt, roster: receipt.roster.filter(({ role }) => role !== 'coordinator') };
  };
  const { swarm_id, roster, head } = await status();
  assert.equal(swarm_id, 'conversation-' + createHash('sha256').update(context.host_conversation_id).digest('hex'));
  assert.equal(head, 0);
  assert.deepEqual(new Set(roster.map((w) => w.session_id)), new Set([a.session_id, b.session_id]));
  assert.ok(roster.every((w) => w.state === 'working'));
  await until(() => [a, b].every((r) => readRun(r.run_id, f.sessionsDir).turn_id));
  for (const run of [a, b]) {
    const active = getRunLineage(run.run_id, f);
    assert.equal(active.swarm_id, swarm_id);
    assert.equal(active.native_session_id, readRun(run.run_id, f.sessionsDir).codex_thread_id);
  }
  for (const { params } of packets.filter((p) => p.method === 'thread/start')) {
    assert.deepEqual(params.dynamicTools.map((tool) => tool.name), ['ask_gemini']);
  }
  for (const { params } of packets.filter((p) => p.method === 'turn/start')) {
    assert.doesNotMatch(params.input[0].text, /board_post|board_read|Shared message board protocol/);
  }
  await Promise.all([interruptRun(a.run_id, f), interruptRun(b.run_id, f)]);
  const source = roster.find((w) => w.session_id === a.session_id).name;
  await until(async () => (await status()).roster.every((w) => w.state === 'stopped'));
  const resume = await run('Finish inspection', a.session_id);
  const done = await waitForRun(resume.run_id, { ...f, timeoutSec: 10 });
  assert.equal(done.state, 'finished', done.error);
  const worker = await until(async () => {
    const w = await getWorker(swarm_id, source, f);
    return w.state === 'waiting' && w;
  });
  assert.equal(worker.run_id, resume.run_id);
  assert.equal(worker.model, done.result.model);
  assert.equal(worker.source_cwd, f.cwd);
  assert.equal((await status()).roster.length, 2);
  assert.equal((await status()).head, 0);
  const add = await handleSwarmAction({ ...context, action: 'add', name: 'reviewer', brief: 'Review findings', fork: source }, f);
  assert.equal(add.swarm_id, swarm_id);
  const child = await waitForRun(add.workers[0].run_id, { ...f, timeoutSec: 10 });
  assert.equal(child.fork_mode, 'native', child.error);
  const lineage = getRunLineage(child.run_id, f);
  assert.deepEqual([lineage.parent_run_id, lineage.root_run_id, lineage.parent_worker,
    lineage.parent_native_session_id, lineage.protocol, lineage.fork_mode],
  [resume.run_id, a.run_id, source, done.codex_thread_id, 'openai-responses', 'native']);
  assert.equal(lineage.cache_group_key, getRunLineage(a.run_id, f).cache_group_key);
  assert.equal(packets.find((p) => p.method === 'thread/fork').params.threadId, done.codex_thread_id);
  await until(async () => (await getWorker(swarm_id, 'reviewer', f)).state === 'waiting');
  const childMcp = packets.find((p) => p.method === 'thread/fork').params.config.mcp_servers.gempt_board;
  assert.equal(childMcp.env.GEMPT_SWARM_ID, swarm_id);
  assert.equal(childMcp.env.GEMPT_AGENT_NAME, 'reviewer');
  assert.equal(childMcp.env.GEMPT_RUN_ID, child.run_id);
  assert.equal(childMcp.env.GEMPT_SESSIONS_DIR, f.sessionsDir);
  assert.match(childMcp.args[0], /board-mcp\.mjs$/);
  const wake = await handleBoardAction({ ...context, action: 'post', text: '@reviewer verify the board' }, f);
  const woke = await waitForRun(wake.deliveries[0].run_id, { ...f, timeoutSec: 10 });
  assert.equal(woke.state, 'finished', woke.error);
  assert.deepEqual(packets.filter((p) => p.method === 'thread/resume').at(-1).params.config.mcp_servers.gempt_board, {
    ...childMcp, env: { ...childMcp.env, GEMPT_RUN_ID: woke.run_id },
  });
  controls.rejectForkConfig = true;
  const fallback = await handleSwarmAction({ ...context, action: 'add', name: 'fallback', brief: 'Review', fork: source }, f);
  const fresh = await waitForRun(fallback.workers[0].run_id, { ...f, timeoutSec: 10 });
  assert.equal(fresh.fork_mode, 'summary_fallback', fresh.error);
  assert.ok(packets.filter((p) => p.method === 'thread/start').at(-1).params.dynamicTools.some((tool) => tool.name === 'board_post'));
  assert.match(packets.filter((p) => p.method === 'turn/start').at(-1).params.input[0].text, /Prior finding: verify the result/);
  const unbound = await dispatchRun({ cwd: f.cwd, goal: 'No host' }, f);
  assert.equal(readRun(unbound.run_id, f.sessionsDir).swarm_id, undefined);
  const summary = await readOverview(f);
  const saved = await updateSettings({ work_split: 'balanced' }, f);
  assert.equal(saved.ok, true);
  const policy = JSON.parse(readFileSync(f.configPath)).routing_policy;
  for (const key of Object.keys(retired)) {
    assert.equal(Object.hasOwn(summary, key), false);
    assert.equal(Object.hasOwn(policy, key), false);
  }
});

test('Codex swarm fork, fallback and warm wake use the right identities, prefixes and turn-local cache usage', async (t) => {
  const f = fixture(t);
  const breakdown = (inputTokens) => ({ inputTokens, outputTokens: 0, cachedInputTokens: inputTokens / 2, totalTokens: inputTokens });
  const controls = { registrations: [], usage: [
    { total: breakdown(120), last: breakdown(20) },
    { total: breakdown(150), last: breakdown(30) },
  ] };
  const packets = await appFixture(t, f, controls);
  const swarm_id = 'fork-test';
  const add = async (name, fork) => {
    const { workers: [worker] } = await handleSwarmAction({ action: 'add', swarm_id, name, cwd: f.cwd,
      brief: name === 'alpha' ? 'Original private assignment' : 'Shared assignment', cli: 'codex', fork }, f);
    const run = await waitForRun(worker.run_id, { ...f, timeoutSec: 10 });
    assert.equal(run.state, 'finished', run.error);
    await until(async () => (await getWorker(swarm_id, name, f)).state === 'waiting');
    return run;
  };
  const lastPrompt = () => packets.filter((p) => p.method === 'turn/start').at(-1).params.input[0].text;
  const parent = await add('alpha');
  assert.equal(parent.result.usage.input_tokens, 150);
  const child = await add('bravo', 'alpha');
  assert.equal(packets.find((p) => p.method === 'thread/fork').params.threadId, parent.codex_thread_id);
  assert.notEqual(child.codex_thread_id, parent.codex_thread_id);
  assert.equal(child.fork_mode, 'native');
  assert.equal((await getWorker(swarm_id, 'bravo', f)).fork_mode, 'native');
  assert.match(lastPrompt(), /@alpha's context[\s\S]*Shared assignment[\s\S]*Worker identity: @bravo/);
  assert.doesNotMatch(lastPrompt(), /Original private assignment|Required Report/);
  assert.equal(child.result.usage.input_tokens, 50);
  assert.equal(child.result.usage.cache_hit_ratio, .5);
  const wake = await handleBoardAction({ action: 'post', swarm_id, text: '@bravo check the final diff' }, f);
  const resumed = await waitForRun(wake.deliveries[0].run_id, { ...f, timeoutSec: 10 });
  assert.equal(resumed.codex_thread_id, child.codex_thread_id);
  assert.ok(controls.registrations.some((r) => r.resume && r.threadId === child.codex_thread_id && r.runId === resumed.run_id));
  assert.match(lastPrompt(), /^\[Board #\d+ from @coordinator[\s\S]*board_post/);
  assert.doesNotMatch(lastPrompt(), /## Goal|Required Report/);
  controls.rejectFork = true;
  const fallback = await add('charlie', 'alpha');
  assert.deepEqual(controls.registrations.map(({ threadId, parentThreadId, swarmId, model }) =>
    [threadId, parentThreadId, swarmId, model]), [
    [parent.codex_thread_id, null, swarm_id, parent.model],
    [child.codex_thread_id, parent.codex_thread_id, swarm_id, child.model],
    [resumed.codex_thread_id, null, swarm_id, resumed.model],
    [fallback.codex_thread_id, null, swarm_id, fallback.model],
  ]);
  assert.equal(fallback.fork_mode, 'summary_fallback');
  assert.match(lastPrompt(), /Original private assignment/);
  assert.doesNotMatch(lastPrompt(), /@alpha's context/);
  const instructions = packets.filter((p) => /^thread\/(start|resume|fork)$/.test(p.method)).map((p) => p.params.developerInstructions);
  assert.equal(new Set(instructions).size, 1);
  assert.doesNotMatch(instructions[0], /fork-test|alpha|bravo/);
});

test('switching a swarm worker from Claude to Codex carries its last result into a fresh thread', async (t) => {
  const f = fixture(t);
  const packets = await appFixture(t, f);
  mkdirSync(f.sessionsDir, { recursive: true });
  const session_id = 'exec-previous-claude';
  writeFileSync(join(f.sessionsDir, session_id + '.json'), JSON.stringify({
    session_id, mode: 'exec', cli: 'claude', cwd: f.cwd, sandbox: 'workspace-write', codex_thread_id: 'claude-native',
    turns: [{ role: 'user', content: 'Trace startup' }, { role: 'assistant', content: 'The issue is in initialization' }],
  }));
  const run = await dispatchRun({ cli: 'codex', model: 'codex/gpt-6.2-sol', cwd: f.cwd, session_id,
    swarm_id: 'switch-test', swarm_agent: 'alpha', goal: '[Board #11 from @coordinator] Continue' }, f);
  assert.equal(run.state, 'finished');
  assert.ok(packets.some((p) => p.method === 'thread/start'));
  assert.match(packets.find((p) => p.method === 'turn/start').params.input[0].text, /Carryover[\s\S]*The issue is in initialization/);
});

test('legacy Codex resume and fork without explicit dynamic board marker attach gempt_board MCP and expose board telemetry', async (t) => {
  const f = fixture(t);
  const packets = await appFixture(t, f);
  const context = { cwd: f.cwd, host_conversation_id: 'conversation-board-compat' };

  // 1) Implicit solo gpt_run remains non-board-capable and marked implicit in swarm status.
  const soloRes = await handleTool('gpt_run', { ...context, cli: 'codex', goal: 'Solo task' }, f.env, f);
  const soloReceipt = JSON.parse(soloRes.content[0].text);
  const soloRun = await waitForRun(soloReceipt.run_id, { ...f, timeoutSec: 10 });
  assert.equal(soloRun.state, 'finished', soloRun.error);
  assert.equal(soloRun.implicit_swarm_worker, true);
  assert.equal(soloRun.board_tool_mode, 'none');
  assert.equal(soloRun.board_capable, false);
  const status1 = await handleSwarmAction({ ...context, action: 'status' }, f);
  const soloWorker = status1.roster.find((w) => w.session_id === soloReceipt.session_id);
  assert.equal(soloWorker.implicit, true);
  assert.equal(soloWorker.board_capable, false);

  // 2) Explicit swarm add gets dynamic tools and board_capable=true.
  const added = await handleSwarmAction({ ...context, action: 'add', name: 'scout', brief: 'Inspect module' }, f);
  const scoutRun = await waitForRun(added.workers[0].run_id, { ...f, timeoutSec: 10 });
  assert.equal(scoutRun.state, 'finished', scoutRun.error);
  assert.equal(scoutRun.board_tool_mode, 'dynamic');
  assert.equal(scoutRun.board_capable, true);
  const scoutFinished = await until(() => {
    const r = readRun(scoutRun.run_id, f.sessionsDir);
    return Number.isInteger(r.board_posts_auto) && r;
  });
  assert.equal(scoutFinished.board_posts_auto, 1);
  assert.equal(scoutFinished.board_posts_worker, 0);

  // 3) Simulate legacy session lacking codex_dynamic_board_tools marker: resume MUST attach gempt_board MCP fallback.
  const scoutSessionFile = join(f.sessionsDir, `${scoutRun.session_id}.json`);
  const scoutSession = JSON.parse(readFileSync(scoutSessionFile, 'utf8'));
  delete scoutSession.codex_dynamic_board_tools;
  writeFileSync(scoutSessionFile, JSON.stringify(scoutSession));
  const resumed = await dispatchRun({
    cli: 'codex', model: scoutRun.model, cwd: f.cwd, session_id: scoutRun.session_id,
    swarm_id: added.swarm_id, swarm_agent: 'scout', goal: 'Continue with board MCP fallback',
  }, f);
  assert.equal(resumed.state, 'finished', resumed.error);
  const resumedState = readRun(resumed.run_id, f.sessionsDir);
  assert.equal(resumedState.board_tool_mode, 'mcp');
  assert.equal(resumedState.board_capable, true);
  const resumePacket = packets.filter((p) => p.method === 'thread/resume').at(-1);
  assert.equal(resumePacket.params.config.mcp_servers.gempt_board.env.GEMPT_AGENT_NAME, 'scout');

  // 4) Native fork from a legacy parent session lacking codex_dynamic_board_tools MUST also attach gempt_board MCP fallback.
  const legacySession = JSON.parse(readFileSync(scoutSessionFile, 'utf8'));
  delete legacySession.codex_dynamic_board_tools;
  writeFileSync(scoutSessionFile, JSON.stringify(legacySession));
  const forked = await handleSwarmAction({ ...context, action: 'add', name: 'child', brief: 'Follow up', fork: 'scout' }, f);
  const childRun = await waitForRun(forked.workers[0].run_id, { ...f, timeoutSec: 10 });
  assert.equal(childRun.fork_mode, 'native', childRun.error);
  assert.equal(childRun.board_tool_mode, 'mcp');
  assert.equal(childRun.board_capable, true);
  const forkPacket = packets.filter((p) => p.method === 'thread/fork').at(-1);
  assert.equal(forkPacket.params.config.mcp_servers.gempt_board.env.GEMPT_AGENT_NAME, 'child');
  const status2 = await handleSwarmAction({ ...context, action: 'status' }, f);
  const childWorker = status2.roster.find((w) => w.name === 'child');
  assert.equal(childWorker.implicit, false);
  assert.equal(childWorker.board_capable, true);
});

test('a dispatched Swarm branch uses its isolated checkout across real worker turns', async (t) => {
  const f = fixture(t);
  const packets = await appFixture(t, f);
  git(f.cwd, 'init', '-q');
  writeFileSync(join(f.cwd, 'branch-fixture.txt'), 'source');
  git(f.cwd, 'add', '.');
  git(f.cwd, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test', 'commit', '-qm', 'test branch workspace');
  const opts = { ...f, env: { ...f.env, GEMPT_WORKTREES_DIR: join(f.root, 'branch-workspaces') } };
  const args = { swarm_id: 'branch-dispatch', cwd: f.cwd };
  const added = await handleSwarmAction({ ...args, action: 'add', name: 'reviewer', brief: 'Review the fixture', workspace: 'branch', cli: 'codex' }, opts);
  const worker = added.workers[0];
  const first = await waitForRun(worker.run_id, { ...opts, timeoutSec: 10 });
  assert.equal(first.state, 'finished', first.error);
  assert.equal(first.cwd, worker.worktree_path);
  assert.equal(first.source_cwd, f.cwd);
  assert.equal(packets.find((packet) => packet.method === 'thread/start')?.params.cwd, worker.worktree_path);
  await until(async () => (await getWorker(args.swarm_id, worker.name, opts)).state === 'waiting');
  const wake = await handleBoardAction({ ...args, action: 'post', text: '@reviewer Verify the branch' }, opts);
  assert.equal(wake.deliveries[0].mode, 'wake', JSON.stringify(wake.deliveries));
  const again = await waitForRun(wake.deliveries[0].run_id, { ...opts, timeoutSec: 10 });
  assert.equal(again.state, 'finished', again.error);
  assert.equal(again.cwd, first.cwd);
  assert.equal(again.session_id, first.session_id);
});
