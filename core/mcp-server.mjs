import { createInterface } from 'node:readline';
import { readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readJsonSafe, resolveConfigPath, resolveSessionsDir } from '../magpie_plugin/lib/backend.mjs';
import { configuredConsultTimeoutSec, GEMPT_VERSION } from './config.mjs';
import { readHostContext } from './host-context.mjs';
import { LAUNCHER_META, LAUNCHER_PATH, LAUNCHER_TYPE, LAUNCHER_URI, SIDECAR_URI } from './mcp-app.mjs';
import { scopedRunRecords } from './live-service.mjs';
import { ACTIVE_STATES, buildWaitCommand, dispatchRun, interruptRun, pendingToolCalls, readRun, refreshRun, replyToolCall, runWithinRoot, steerRun, validateExecTimeoutSec } from './runs.mjs';
import { isPlainObject, lockStaleMsForBudget, parseToolArguments, resolvePath, validateSessionId, withLock } from './state.mjs';
import { handleBoardAction, handleSwarmAction, routeBoardDeliveries } from './swarm-engine.mjs';
import { getSwarmOverview } from './swarm-bus.mjs';
import { startSwarmMonitor } from './swarm-monitor.mjs';
import { TOOLS } from './tool-schemas.mjs';
import { normalizeTitle } from '../magpie_plugin/lib/activity.mjs';
export { TOOLS };

const textResult = (data, extras) => ({ content: [{ type: 'text', text: JSON.stringify(data) }], ...extras });
const launcherSummary = (fields) => ({ ...fields, sidecar_uri: SIDECAR_URI, created_at_ms: Date.now() });
function runStatus(record, options) {
  const item = { id: record.run_id, st: record.state, sid: record.session_id, title: record.title };
  if (ACTIVE_STATES.has(record.state)) item.wait = buildWaitCommand(record.run_id, options);
  if (record.remote_turn_unconfirmed) item.unconfirmed = true;
  const asks = pendingToolCalls(record).map((pending) => ({ ...pending, q: pending.arguments?.question }));
  if (asks.length) item.ask = asks[0];
  if (asks.length > 1) item.asks = asks;
  if (record.error) item.err = record.error;
  if (record.result?.text) item.msg = record.result.text;
  return item;
}

async function control(args, options) {
  if (Object.hasOwn(args, 'wait_sec')) {
    throw new Error('gpt_ctl does not support wait_sec polling; use `gempt-wait <run_id>` via background run_command.');
  }
  const action = String(args.action || 'status').trim();
  if (!['status', 'steer', 'interrupt', 'reply'].includes(action)) throw new Error(`Invalid action: ${action}`);
  const runId = typeof args.run_id === 'string' && args.run_id ? args.run_id : null;
  if (!runId && action !== 'status') throw new Error(`run_id is required for action='${action}'.`);
  const record = runId && readRun(runId, options.sessionsDir);
  if (record && !runWithinRoot(record, options.root)) throw new Error('Run outside host workspace');
  if (action === 'steer') return textResult(await steerRun(runId, String(args.message || ''), options));
  if (action === 'interrupt') return textResult(await interruptRun(runId, options));
  if (action === 'reply') return textResult(replyToolCall(runId, String(args.call_id || ''), String(args.message || ''), options));
  if (record) return textResult(runStatus(await refreshRun(record, options), options));
  const { items, routing } = scopedRunRecords({ host_conversation_id: args.host_conversation_id,
    scope: args.scope === 'all' ? 'all' : 'session' }, options);
  const active = [], recent = [];
  for (const record of items) {
    const run = await refreshRun(record, options);
    (ACTIVE_STATES.has(run.state) ? active : recent).push({ id: run.run_id, st: run.state, t: run.title });
  }
  return textResult({ split: routing.profile, target: routing.gpt_share, active, recent });
}

export async function handleTool(name, args, env = process.env, overrides = {}, callCtx) {
  const options = {
    configPath: resolveConfigPath(env.GEMPT_CONFIG), sessionsDir: resolveSessionsDir(env.GEMPT_SESSIONS_DIR),
    root: env.GEMPT_WORKSPACE_ROOT?.trim() ? resolvePath(env.GEMPT_WORKSPACE_ROOT.trim()) : null, env,
    magpieConfigDir: env.MAGPIE_CONFIG_DIR, ...overrides,
  };
  if (name === 'gpt_ctl') return control(args, options);
  if (name === 'gpt_swarm') {
    const data = await handleSwarmAction(args, {
      ...options,
      agent: typeof callCtx?.agent === 'string' ? callCtx.agent : undefined,
    });
    const action = String(args.action || 'status').trim();
    if (action === 'models') return textResult(data);
    const overview = action === 'status' ? null
      : await getSwarmOverview(data.swarm_id, { sessionsDir: options.sessionsDir, messageLimit: 0 }).catch(() => null);
    const roster = (data.roster ?? overview?.roster ?? data.workers ?? []).filter(({ state, role }) => state !== 'killed' && role !== 'coordinator');
    return textResult(data, { structuredContent: launcherSummary({
      kind: 'swarm', action, title: data.title ?? overview?.swarm.title ?? data.goal?.title ?? null,
      state: roster.some(({ state }) => state === 'working' || state === 'waiting_reply') ? 'working' : 'idle',
      swarm_id: data.swarm_id, member_count: roster.length, auto_open_panel: action === 'add',
    }) });
  }
  if (name === 'gpt_board') {
    const board = await handleBoardAction(args, {
      ...options,
      agent: typeof callCtx?.agent === 'string' ? callCtx.agent : undefined,
    });
    return textResult(board);
  }
  if (!['gpt_run', 'gpt_ask'].includes(name)) throw new Error(`Unknown tool: ${name}`);
  const goal = String(args.goal || '').trim();
  if (!goal) throw new Error('`goal` is required.');
  const cwd = options.root && (args.cwd == null || typeof args.cwd === 'string')
    ? resolve(options.root, args.cwd?.trim() || '.') : typeof args.cwd === 'string' ? args.cwd.trim() : args.cwd;
  const sessionId = String(args.session_id || '').trim();
  if (name === 'gpt_ask') {
    const context = readHostContext(options.configPath, cwd);
    const hostId = args.host_conversation_id || context.conversation_id;
    const family = args.gemini_family || context.gemini_family;
    const { createExecutionTools } = await import('../magpie_plugin/lib/tools.mjs');
    const consult = createExecutionTools({ ...options,
      hostCwdBound: Boolean(context.recorded_at_ms && hostId === context.conversation_id) }).consult;
    if (!cwd && args.file_paths != null) throw new Error('cwd is required when providing file_paths.');
    const execute = () => consult.execute({ goal, tier: args.tier, cwd, file_paths: args.file_paths, max_output_tokens: args.max_output_tokens,
      session_id: sessionId, gemini_family: family, mcp: true, host_conversation_id: hostId }, callCtx);
    const cfg = readJsonSafe(options.configPath);
    const consultBudgetSec = configuredConsultTimeoutSec(cfg?.limits);
    const res = sessionId
      ? await withLock(join(options.sessionsDir, `${validateSessionId(sessionId)}.node.lock`), execute,
        { staleMs: lockStaleMsForBudget(consultBudgetSec) })
      : await execute();
    return textResult({ session_id: res.session_id, model: res.served_model, tier: res.tier, reasoning_effort: res.reasoning_effort,
      text: res.text, warnings: res.warnings });
  }
  const access = String(args.access || 'change').trim();
  if (!['inspect', 'change'].includes(access)) throw new Error(`Invalid access: ${access}`);
  if (access === 'inspect' && args.workspace === 'share') {
    throw new Error("workspace='share' is not allowed with read-only access ('inspect'); use workspace='inherit' for read-only inspection.");
  }
  const timeoutSec = validateExecTimeoutSec(args.timeout_sec);
  const syncBudget = Number(env.GEMPT_GPT_RUN_SYNC_SEC);
  const waitSec = Number.isFinite(syncBudget) && syncBudget > 0 ? Math.min(syncBudget, 10) : 0;
  const res = await dispatchRun({ goal, cwd, access, workspace: args.workspace, title: args.title, tier: args.tier,
    ...(args.cli ? { cli: args.cli } : {}),
    ...(timeoutSec !== undefined ? { timeout_sec: timeoutSec } : {}),
    host_conversation_id: args.host_conversation_id, gemini_family: args.gemini_family, mcp: true, session_id: sessionId,
    background: args.background ?? waitSec === 0, sandbox: access === 'inspect' ? 'read-only' : undefined,
    expected_report: 'Report concisely: changed files, verification commands/results, and any remaining blockers. Omit verbose raw logs.',
  }, { ...options, waitSec, agent: typeof callCtx?.agent === 'string' ? callCtx.agent : undefined });
  const launcherResult = (isError) => ({ isError, ...(!isError ? {
    structuredContent: launcherSummary({
      kind: 'run', action: 'start', title: res.title ?? normalizeTitle(args.title, goal, cwd),
      state: res.state, run_id: res.run_id, member_count: 1, auto_open_panel: true,
    }),
  } : {}) });
  if (ACTIVE_STATES.has(res.state) || res.state === 'interrupted') {
    const wait = buildWaitCommand(res.run_id, options);
    return textResult({ run_id: res.run_id, session_id: res.session_id, state: res.state, wait_command: wait,
      execution_timeout_sec: res.execution_timeout_sec, deadline: res.deadline,
      ...(res.cli ? { cli: res.cli } : {}),
      text: res.state === 'waiting_gemini'
        ? `Run ${res.run_id} is waiting for ask_gemini reply (use gpt_ctl action='reply').`
        : `Run ${res.run_id} started. Execute \`${wait}\` via run_command(WaitMsBeforeAsync=500) for zero-poll wakeup.`,
      ...(res.worktree_path ? { worktree_path: res.worktree_path } : {}),
    }, launcherResult(res.state === 'interrupted'));
  }
  const { run_id, session_id, cli, model, tier, reasoning_effort, worktree_path, verification, usage, text } = res;
  return textResult({ run_id, session_id, ...(cli ? { cli } : {}), model, tier, reasoning_effort, worktree_path, verification, usage,
    execution_timeout_sec: res.execution_timeout_sec, deadline: res.deadline, text }, launcherResult(['failed', 'unverified'].includes(verification?.status)));
}

const errorResponse = (id, code, message, data) => ({ jsonrpc: '2.0', id, error: { code, message, data } });

async function dispatch(request, env) {
  const { id, method, params = {} } = request;
  if (id == null || String(method).startsWith('notifications/')) return;
  const response = (result) => ({ jsonrpc: '2.0', id, result });
  try {
    switch (method) {
      case 'initialize':
        return response({ protocolVersion: params?.protocolVersion || '2024-11-05',
          capabilities: { tools: { listChanged: false }, resources: {} },
          serverInfo: { name: 'gempt', version: GEMPT_VERSION } });
      case 'ping': return response({});
      case 'tools/list': return response({ tools: TOOLS });
      case 'resources/list':
        return response({ resources: [{ uri: LAUNCHER_URI, name: 'GemPT launcher', mimeType: LAUNCHER_TYPE, _meta: LAUNCHER_META }] });
      case 'resources/read':
        if (params?.uri !== LAUNCHER_URI) return errorResponse(id, -32002, 'Resource not found', { uri: params?.uri });
        return response({ contents: [{ uri: LAUNCHER_URI, mimeType: LAUNCHER_TYPE, _meta: LAUNCHER_META,
          text: await readFile(new URL(LAUNCHER_PATH, import.meta.url), 'utf8') }] });
      case 'tools/call': {
        const name = String(params?.name || '');
        const args = parseToolArguments(params?.arguments);
        try {
          return response(await handleTool(name, args, env));
        } catch (error) {
          return response({ content: [{ type: 'text', text: `Error in ${name}: ${error.message}` }], isError: true });
        }
      }
      default: return errorResponse(id, -32601, `Method not found: ${method}`);
    }
  } catch (error) {
    return errorResponse(id, -32603, error.message);
  }
}

export async function serveStdio(input = process.stdin, output = process.stdout, env = process.env) {
  const pending = new Set();
  const sessionsDir = resolveSessionsDir(env.GEMPT_SESSIONS_DIR);
  const stopMonitor = startSwarmMonitor({ sessionsDir, env,
    dispatchAlert: (swarmId) => routeBoardDeliveries(swarmId, { sessionsDir, env }) });
  const write = (message) => {
    if (message) output.write(`${JSON.stringify(message)}\n`);
  };
  const readline = createInterface({ input, crlfDelay: Infinity });
  readline.once('close', stopMonitor);
  for await (const line of readline) {
    if (!line.trim()) continue;
    let request;
    try { request = JSON.parse(line); } catch (error) {
      write(errorResponse(null, -32700, `Parse error: ${error.message}`));
      continue;
    }
    if (!isPlainObject(request)) continue;
    const task = dispatch(request, env).then(write);
    pending.add(task);
    task.finally(() => pending.delete(task));
  }
  await Promise.all(pending);
}

if (process.argv[1] && resolvePath(process.argv[1]) === resolvePath(fileURLToPath(import.meta.url))) await serveStdio();
