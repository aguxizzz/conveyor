'use strict';

// Everything shown here comes from the public API; job types, payloads and errors are client input,
// so the page only ever inserts them as text (see h()), never as HTML.

const REFRESH_MS = 2000;
const STATUSES = ['PENDING', 'RUNNING', 'SUCCEEDED', 'DEAD'];
// A runnable job waiting longer than this means workers aren't keeping up.
const BACKLOG_WARN_SECONDS = 30;

const $ = (id) => document.getElementById(id);
const filters = $('filters');
const jobDialog = $('job-dialog');
const enqueueDialog = $('enqueue-dialog');
const enqueueForm = $('enqueue-form');

let paused = false;
let timer = null;
let generation = 0;
let openJobId = null;

class ApiError extends Error {
	constructor(status, body) {
		super(body?.detail || body?.message || body?.error || `HTTP ${status}`);
		this.status = status;
	}
}

async function api(path, options = {}) {
	const res = await fetch(path, {
		...options,
		headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
	});
	// 404s from ResponseEntity.of() and 204s have no body.
	const body = await res.json().catch(() => null);
	if (!res.ok) {
		throw new ApiError(res.status, body);
	}
	return body;
}

function h(tag, attrs, ...children) {
	const el = document.createElement(tag);
	for (const [key, value] of Object.entries(attrs ?? {})) {
		if (value == null || value === false) continue;
		if (key.startsWith('on')) el.addEventListener(key.slice(2), value);
		else if (key === 'class') el.className = value;
		else el.setAttribute(key, value === true ? '' : value);
	}
	el.append(...children.flat(Infinity).filter((c) => c != null && c !== false).map((c) => (c instanceof Node ? c : String(c))));
	return el;
}

function relative(iso) {
	if (!iso) return '—';
	const seconds = (Date.parse(iso) - Date.now()) / 1000;
	const abs = Math.abs(seconds);
	if (abs < 1) return 'now';
	const text = abs < 60 ? `${Math.round(abs)}s`
		: abs < 3600 ? `${Math.round(abs / 60)}m`
		: abs < 86400 ? `${Math.round(abs / 3600)}h`
		: `${Math.round(abs / 86400)}d`;
	return seconds > 0 ? `in ${text}` : `${text} ago`;
}

function time(iso) {
	return h('time', { datetime: iso, title: iso ? new Date(iso).toLocaleString() : null }, relative(iso));
}

function duration(seconds) {
	if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
	if (seconds < 120) return `${seconds.toFixed(1)} s`;
	return `${Math.round(seconds / 60)} min`;
}

function badge(status) {
	return h('span', { class: `badge ${status}` }, status);
}

function firstLine(text) {
	return text ? text.split('\n', 1)[0] : '';
}

let toastTimer = null;
function toast(message, bad = false) {
	const el = $('toast');
	el.textContent = message;
	el.classList.toggle('bad', bad);
	el.hidden = false;
	clearTimeout(toastTimer);
	toastTimer = setTimeout(() => (el.hidden = true), 3500);
}

// Filters live in the URL, so a filtered view can be bookmarked or shared.

function readFilters() {
	const params = new URLSearchParams(location.search);
	for (const name of ['status', 'queue', 'limit']) {
		const value = params.get(name);
		if (value === null) continue;
		const select = filters.elements[name];
		// An option the page doesn't know yet (a queue with no jobs) is added so the filter still applies.
		if (![...select.options].some((o) => o.value === value)) select.append(h('option', { value }, value));
		select.value = value;
	}
}

function currentFilters() {
	const params = new URLSearchParams();
	for (const name of ['status', 'queue']) {
		if (filters.elements[name].value) params.set(name, filters.elements[name].value);
	}
	if (filters.elements.limit.value !== '50') params.set('limit', filters.elements.limit.value);
	return params;
}

function applyFilters(changes) {
	for (const [name, value] of Object.entries(changes)) filters.elements[name].value = value;
	const query = currentFilters().toString();
	history.replaceState(null, '', query ? `?${query}` : location.pathname);
	refresh();
}

filters.addEventListener('change', () => applyFilters({}));

// Rendering

function renderQueues(stats) {
	$('queues').replaceChildren(...stats.map((q) => {
		const counts = { PENDING: q.pending, RUNNING: q.running, SUCCEEDED: q.succeeded, DEAD: q.dead };
		const waiting = q.oldestReadySeconds;
		return h('article', { class: 'queue' },
			h('div', { class: 'queue-head' },
				h('span', { class: 'queue-name' }, q.queue),
				h('span', { class: `queue-age${waiting > BACKLOG_WARN_SECONDS ? ' warn' : ''}` },
					waiting > 0 ? `oldest ready job waiting ${duration(waiting)}` : 'nothing waiting')),
			h('div', { class: 'counts' }, STATUSES.map((status) =>
				h('button', {
					type: 'button',
					class: `count${counts[status] === 0 ? ' zero' : ''}`,
					'data-status': status,
					title: `Show ${status} jobs in ${q.queue}`,
					onclick: () => applyFilters({ status, queue: q.queue }),
				}, h('strong', null, counts[status].toLocaleString()), h('span', null, status)))));
	}));

	if (stats.length === 0) {
		$('queues').append(h('p', { class: 'empty' }, 'No jobs yet. Enqueue one below.'));
	}

	// Keep the queue filter in sync with the queues that exist, without dropping the current choice.
	const select = filters.elements.queue;
	const known = new Set([...select.options].map((o) => o.value));
	for (const q of stats) {
		if (!known.has(q.queue)) select.append(h('option', { value: q.queue }, q.queue));
	}
}

function retryButton(job) {
	return h('button', {
		type: 'button',
		class: 'small',
		onclick: async (event) => {
			event.stopPropagation();
			event.currentTarget.disabled = true;
			try {
				await api(`/api/jobs/${job.id}/retry`, { method: 'POST' });
				toast(`Job #${job.id} is pending again`);
			}
			catch (e) {
				toast(e.status === 404 ? `Job #${job.id} is no longer dead` : `Retry failed: ${e.message}`, true);
			}
			refresh();
		},
	}, 'Retry');
}

function renderJobs(jobs) {
	$('jobs').replaceChildren(...jobs.map((job) =>
		h('tr', { class: 'clickable', onclick: () => openJob(job.id) },
			h('td', null, h('button', { type: 'button', class: 'link' }, `#${job.id}`)),
			h('td', null, badge(job.status)),
			h('td', { class: 'mono' }, job.type),
			h('td', { class: 'mono' }, job.queue),
			h('td', { class: 'num' }, `${job.attempts} / ${job.maxAttempts}`),
			h('td', null, job.status === 'PENDING' ? time(job.runAt) : '—'),
			h('td', null, time(job.createdAt)),
			h('td', { class: 'mono muted' }, job.lockedBy ?? ''),
			h('td', { class: 'error-cell', title: job.lastError }, firstLine(job.lastError)),
			h('td', { class: 'actions' }, job.status === 'DEAD' ? retryButton(job) : null))));
	$('jobs-empty').hidden = jobs.length > 0;
}

function deleteButton(schedule) {
	// Two clicks instead of confirm(): the first arms the button for a few seconds.
	let armedUntil = 0;
	return h('button', {
		type: 'button',
		class: 'small danger',
		onclick: async (event) => {
			const button = event.currentTarget;
			if (Date.now() > armedUntil) {
				armedUntil = Date.now() + 3000;
				button.classList.add('armed');
				button.textContent = 'Confirm';
				setTimeout(() => {
					button.classList.remove('armed');
					button.textContent = 'Delete';
				}, 3000);
				return;
			}
			button.disabled = true;
			try {
				await api(`/api/schedules/${encodeURIComponent(schedule.name)}`, { method: 'DELETE' });
				toast(`Deleted schedule ${schedule.name}`);
			}
			catch (e) {
				toast(`Delete failed: ${e.message}`, true);
			}
			refresh();
		},
	}, 'Delete');
}

function renderSchedules(schedules) {
	$('schedules').replaceChildren(...schedules.map((s) =>
		h('tr', null,
			h('td', { class: 'mono' }, s.name, s.enabled ? null : [' ', h('span', { class: 'badge off' }, 'DISABLED')]),
			h('td', null, h('code', null, s.cron), h('span', { class: 'muted' }, ` ${s.timeZone}`)),
			h('td', { class: 'mono' }, s.type, h('span', { class: 'muted' }, ` → ${s.queue}`)),
			h('td', null, s.enabled ? time(s.nextRunAt) : '—'),
			h('td', null, time(s.lastRunAt)),
			h('td', { class: 'actions' }, deleteButton(s)))));
	$('schedules-empty').hidden = schedules.length > 0;
}

function renderLeader(leader) {
	const pill = $('leader');
	pill.classList.toggle('warn', !leader);
	pill.textContent = leader ? `scheduler leader ${leader.holder} · term ${leader.term}` : 'no scheduler leader';
	pill.title = leader ? `Lease expires ${new Date(leader.expiresAt).toLocaleTimeString()}` : '';
}

function setLive(state, text) {
	$('live').dataset.state = state;
	$('live-text').textContent = text;
}

// Job details

function renderJob(job) {
	$('job-title').replaceChildren(`Job #${job.id} `, badge(job.status));
	const rows = [
		['Type', h('span', { class: 'mono' }, job.type)],
		['Queue', h('span', { class: 'mono' }, job.queue)],
		['Attempts', `${job.attempts} of ${job.maxAttempts}`],
		['Priority', job.priority],
		['Run at', time(job.runAt)],
		['Worker', job.lockedBy ? h('span', { class: 'mono' }, job.lockedBy) : '—'],
		['Lease until', job.lockedUntil ? time(job.lockedUntil) : '—'],
		['Idempotency key', job.idempotencyKey ? h('span', { class: 'mono' }, job.idempotencyKey) : '—'],
		['Created', time(job.createdAt)],
		['Finished', time(job.finishedAt)],
	];
	$('job-body').replaceChildren(h('div', null,
		h('dl', { class: 'details' }, rows.map(([label, value]) => [h('dt', null, label), h('dd', null, value)])),
		h('p', { class: 'block-title' }, 'Payload'),
		h('pre', null, JSON.stringify(job.payload, null, 2)),
		job.lastError ? [h('p', { class: 'block-title' }, 'Last error'), h('pre', { class: 'error-text' }, job.lastError)] : null,
		job.status === 'DEAD' ? h('div', { class: 'dialog-actions' }, retryButton(job)) : null));
}

async function openJob(id) {
	openJobId = id;
	try {
		renderJob(await api(`/api/jobs/${id}`));
		if (!jobDialog.open) jobDialog.showModal();
	}
	catch (e) {
		openJobId = null;
		toast(e.status === 404 ? `Job #${id} no longer exists` : e.message, true);
	}
}

jobDialog.addEventListener('close', () => (openJobId = null));

// Enqueue

function numberOrNull(value) {
	return value === '' ? null : Number(value);
}

enqueueForm.addEventListener('submit', async (event) => {
	event.preventDefault();
	const form = enqueueForm.elements;
	const error = $('enqueue-error');
	let payload;
	try {
		payload = JSON.parse(form.payload.value || '{}');
	}
	catch (e) {
		error.textContent = `Payload isn't valid JSON: ${e.message}`;
		error.hidden = false;
		return;
	}
	try {
		const job = await api('/api/jobs', {
			method: 'POST',
			body: JSON.stringify({
				type: form.type.value.trim(),
				payload,
				queue: form.queue.value.trim() || null,
				priority: numberOrNull(form.priority.value),
				maxAttempts: numberOrNull(form.maxAttempts.value),
				delaySeconds: numberOrNull(form.delaySeconds.value),
				idempotencyKey: form.idempotencyKey.value.trim() || null,
			}),
		});
		enqueueDialog.close();
		toast(`Enqueued job #${job.id}`);
		refresh();
		openJob(job.id);
	}
	catch (e) {
		error.textContent = `Couldn't enqueue: ${e.message}`;
		error.hidden = false;
	}
});

$('open-enqueue').addEventListener('click', () => {
	$('enqueue-error').hidden = true;
	enqueueDialog.showModal();
	enqueueForm.elements.type.focus();
});

for (const button of document.querySelectorAll('[data-close]')) {
	button.addEventListener('click', () => button.closest('dialog').close());
}

// Refresh loop: one request round at a time, paused while the tab is hidden.

async function refresh() {
	clearTimeout(timer);
	// A refresh started while another is in flight (a filter change) supersedes it, so only one loop runs.
	const run = ++generation;
	try {
		const [stats, jobs, schedules, leader] = await Promise.all([
			api('/api/queues'),
			api(`/api/jobs?${currentFilters()}`),
			api('/api/schedules'),
			api('/api/scheduler/leader').catch((e) => (e.status === 404 ? null : Promise.reject(e))),
		]);
		if (run !== generation) return;
		renderQueues(stats);
		renderJobs(jobs);
		renderSchedules(schedules);
		renderLeader(leader);
		if (openJobId !== null) {
			// Keep the open job live too, so it can be watched going from PENDING to its outcome.
			const job = await api(`/api/jobs/${openJobId}`).catch((e) => (e.status === 404 ? null : Promise.reject(e)));
			if (job && job.id === openJobId) renderJob(job);
		}
		setLive(paused ? 'paused' : 'live', paused ? 'Paused' : `Updated ${new Date().toLocaleTimeString()}`);
	}
	catch (e) {
		setLive('error', `Can't reach the API (${e.message})`);
	}
	if (run === generation && !paused && !document.hidden) {
		timer = setTimeout(refresh, REFRESH_MS);
	}
}

$('toggle-refresh').addEventListener('click', (event) => {
	paused = !paused;
	event.currentTarget.textContent = paused ? 'Resume' : 'Pause';
	refresh();
});

document.addEventListener('visibilitychange', () => {
	if (!document.hidden) refresh();
});

readFilters();
refresh();
